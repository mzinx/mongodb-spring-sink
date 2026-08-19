package com.mzinx.mongodb.sink.listener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mzinx.mongodb.aggregation.dao.PipelineRepository;
import com.mzinx.mongodb.aggregation.model.AggregationSpec;
import com.mzinx.mongodb.aggregation.model.PipelineTemplate;
import com.mzinx.mongodb.aggregation.service.AggregationService;
import com.mzinx.mongodb.changestream.listener.ChangeStreamListener;
import com.mzinx.mongodb.sink.support.EventVariables;
import com.mzinx.mongodb.sink.support.RecomputeCoalescer;
import com.mzinx.mongodb.sink.support.RecomputeTargets;
import com.mzinx.mongodb.sink.support.RecomputeTargets.Target;

/**
 * Generic change-stream processor that maintains a <em>materialized view</em>:
 * whenever the watched source collection changes, it re-runs a configured
 * aggregation pipeline whose final stage {@code $merge}s the result into an
 * output collection (replacing documents by {@code _id}).
 * <p>
 * The listener is fully generic — nothing about a particular view is hardcoded.
 * The aggregation source is the collection that produced the event, and the
 * pipeline to run is taken from the {@code attributes.outputPipeline} of the
 * change stream that triggered it (the {@link PipelineTemplate} id in the
 * {@code _pipelines} collection). One bean can therefore back many views, each
 * configured by its own change stream.
 * <p>
 * Register a change stream with {@code listener = }{@value #BEAN_NAME} and an
 * {@code attributes.outputPipeline} naming the pipeline template to run. The
 * change-stream library resolves this bean by name and drives it per event.
 * <p>
 * The recompute is a full recompute keyed by {@code _id} via the pipeline's
 * terminal {@code $merge} ({@code whenMatched: replace}). Removing output
 * documents whose source data has disappeared is out of scope here — use a
 * MongoDB TTL index on the output collection for that housekeeping.
 * <p>
 * By default each change event recomputes immediately. A stream may opt into
 * <b>coalesced (debounced) recompute</b> via {@link #ATTR_RECOMPUTE_DEBOUNCE_MS}
 * (and optionally {@link #ATTR_RECOMPUTE_MAX_DELAY_MS}) so a burst of events
 * collapses into a single recompute — the fix for repeated full-collection
 * recomputation (see {@link RecomputeCoalescer}). Leave it off for per-document /
 * scoped recomputes, which are already O(1) and must run immediately.
 */
@Component(MaterializedViewListener.BEAN_NAME)
public class MaterializedViewListener implements ChangeStreamListener<Document> {

    public static final String BEAN_NAME = "materializedViewListener";

    /** Attribute key naming the output pipeline to run (on the change stream config). */
    public static final String ATTR_OUTPUT_PIPELINE = "outputPipeline";

    /**
     * Attribute holding the terminal write stage(s) to append at runtime — a list
     * of one or more targets, so one recompute can fan out to several collections
     * (e.g. a daily / weekly / monthly period rollup, or two different
     * {@code $group}s each {@code $merge}-ing into their own collection). A single
     * target is simply a one-element list.
     * <p>
     * This lets a shared, write-stage-free {@link PipelineTemplate} be reused by
     * many streams, each writing to its own target(s): the template supplies the
     * transformation body and each stream carries its own write configuration here.
     * <p>
     * The value is a list; each entry is a map with:
     * <ul>
     * <li><b>{@code writeStage}</b> (required) — the terminal {@code $merge}/{@code $out}
     * for this target, e.g.
     * {@code {"$merge": {"into": "orders_view", "whenMatched": "replace", "whenNotMatched": "insert"}}}
     * or {@code {"$out": "orders_snapshot"}}; and</li>
     * <li><b>{@code variables}</b> (optional) — a map of extra placeholder variables
     * bound for this target only (merged over the stream's shared variables), so the
     * shared template body can be parameterized per target via {@code {"_ph": "..."}}.</li>
     * </ul>
     * For each entry the listener runs the resolved template body with that entry's
     * variables and its appended {@code writeStage} — one aggregation pass per target
     * (a pipeline can only end in one {@code $merge}/{@code $out}, so N targets means
     * N passes). Failures in one target are logged and do not abort the others.
     * <p>
     * When this attribute is absent the template must be <em>self-contained</em>
     * (already end in its own {@code $merge}/{@code $out}); otherwise the recompute
     * produces no output. Example:
     * <pre>
     * writeStages = [
     *   { variables: { period: "day"   }, writeStage: { $merge: { into: "ordersByDay",   ... } } },
     *   { variables: { period: "week"  }, writeStage: { $merge: { into: "ordersByWeek",  ... } } },
     *   { variables: { period: "month" }, writeStage: { $merge: { into: "ordersByMonth", ... } } }
     * ]
     * </pre>
     */
    public static final String ATTR_WRITE_STAGES = "writeStages";

    /**
     * Optional attribute naming the collection to aggregate over. When absent (or
     * blank) the pipeline runs against the collection that produced the change
     * event (the previous, and still default, behavior). Setting it lets a view
     * be recomputed from a different collection than the one being watched — the
     * change event merely <em>triggers</em> the recompute and does not dictate the
     * aggregation source.
     */
    public static final String ATTR_AGGREGATION_COLLECTION = "aggregationCollection";

    /**
     * Optional attribute enabling <b>coalesced (debounced) recompute</b>: the
     * millisecond quiet-window over which a burst of change events collapses into a
     * single recompute (see {@link RecomputeCoalescer}). Absent or {@code <= 0}
     * means the recompute runs <b>immediately</b> per event (the default, unchanged
     * behavior).
     * <p>
     * Enable this only for <b>full-collection recomputes</b> (e.g. a {@code $group}
     * rollup), where re-running per event is wasteful and the result is idempotent.
     * Do NOT enable it for per-document / scoped recomputes (those keyed off
     * {@code {"_ph": "event.documentKey._id"}}) — each event there is distinct,
     * O(1) work and must run immediately. Coalescing trades a bounded latency and
     * the strict resume-token guarantee for far less repeated computation.
     * <p>
     * (Keyed without a dot: it is persisted as a BSON map key in the change stream
     * config's {@code attributes}, and BSON map keys may not contain dots.)
     */
    public static final String ATTR_RECOMPUTE_DEBOUNCE_MS = "recomputeDebounceMs";

    /**
     * Optional cap (ms) on how long a continuously-busy coalesced stream may be
     * held back before it is forced to flush, so a steady event rate still produces
     * output at least this often. {@code 0}/absent = no cap. Only meaningful
     * together with {@link #ATTR_RECOMPUTE_DEBOUNCE_MS}.
     */
    public static final String ATTR_RECOMPUTE_MAX_DELAY_MS = "recomputeMaxDelayMs";

    /**
     * Prefix under which the change event's fields are exposed as pipeline
     * placeholder variables. A pipeline can reference them with dotted paths, e.g.
     * <pre>{"_ph": "event.documentKey._id"}</pre>,
     * <pre>{"_ph": "event.fullDocument.status"}</pre> or
     * <pre>{"_ph": "event.operationType"}</pre>. The available sub-fields are
     * {@code operationType}, {@code documentKey}, {@code ns} (with {@code db} /
     * {@code coll}), {@code fullDocument} and {@code fullDocumentBeforeChange}.
     */
    public static final String VAR_EVENT = EventVariables.VAR_EVENT;

    /**
     * Prefix under which the change stream config's custom {@code attributes} are
     * exposed as pipeline placeholder variables, e.g.
     * <pre>{"_ph": "attr.tenantId"}</pre>. Every entry in the stream's
     * {@code attributes} map is available here in addition to the reserved keys
     * ({@code outputPipeline}, {@code writeStages}, {@code aggregationCollection}).
     */
    public static final String VAR_ATTR = EventVariables.VAR_ATTR;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final PipelineRepository pipelineRepository;
    private final AggregationService aggregationService;
    /**
     * Debounces full recomputes per stream for streams that opt in via
     * {@link #ATTR_RECOMPUTE_DEBOUNCE_MS}. Runs the same {@link #recompute} the
     * immediate path uses, just once per coalesced window.
     */
    private final RecomputeCoalescer coalescer = new RecomputeCoalescer(this::recomputeSafely);

    MaterializedViewListener(PipelineRepository pipelineRepository, AggregationService aggregationService) {
        this.pipelineRepository = pipelineRepository;
        this.aggregationService = aggregationService;
    }

    @Override
    public void onEvent(String streamId, Map<String, Object> attributes, ChangeStreamDocument<Document> event) {
        long debounceMs = longAttr(attributes, ATTR_RECOMPUTE_DEBOUNCE_MS);
        if (debounceMs > 0) {
            // Coalesced path: record the request; a single recompute runs after the
            // quiet window. Returns immediately so a burst collapses into one run.
            long maxDelayMs = longAttr(attributes, ATTR_RECOMPUTE_MAX_DELAY_MS);
            logger.debug("Stream '{}' change coalesced (debounceMs={}, maxDelayMs={})",
                    streamId, debounceMs, maxDelayMs);
            coalescer.submit(streamId, attributes, event, debounceMs, maxDelayMs);
            return;
        }
        // Immediate path (default, unchanged): recompute now, on the cursor thread.
        logger.info("Source change detected on stream '{}' ({}), recomputing view. Attribute keys={}",
                streamId, event.getOperationType() != null ? event.getOperationType().getValue() : "?",
                attributes != null ? attributes.keySet() : null);
        recomputeSafely(streamId, attributes, event);
    }

    /** Runs {@link #recompute} but never lets a failure escape (would stop the stream). */
    private void recomputeSafely(String streamId, Map<String, Object> attributes,
            ChangeStreamDocument<Document> event) {
        try {
            recompute(streamId, attributes, event);
        } catch (RuntimeException e) {
            logger.error("Recompute for stream '{}' failed: {}", streamId, e.getMessage(), e);
        }
    }

    /** Stops the coalescer's background scheduler on shutdown. */
    @PreDestroy
    void shutdown() {
        coalescer.shutdown();
    }

    /**
     * Recomputes the output collection for the given change stream.
     * <p>
     * The aggregation runs against the {@link #ATTR_AGGREGATION_COLLECTION}
     * attribute when set, otherwise against the collection that produced the
     * event. Change-event fields (under {@code event.*}) and the stream's custom
     * attributes (under {@code attr.*}) are exposed as pipeline placeholder
     * variables, so a pipeline can be parameterized per event via
     * {@code {"_ph": "event.documentKey._id"}} and similar.
     *
     * @param streamId   the change stream id (for diagnostics / config lookup)
     * @param attributes the free-form attributes carried with the event
     * @param event      the triggering change event; may be {@code null} for a
     *                   direct, non-event-driven recompute
     * @throws IllegalStateException if the stream has no {@code outputPipeline}
     *                               attribute, or it names a pipeline that does not
     *                               exist — both are misconfiguration and must be
     *                               fixed before the listener can run.
     */
    public synchronized void recompute(String streamId, Map<String, Object> attributes,
            ChangeStreamDocument<Document> event) {
        String pipelineName = attr(attributes, ATTR_OUTPUT_PIPELINE);
        if (pipelineName.isEmpty())
            throw new IllegalStateException("Change stream '" + streamId + "' is missing the required '"
                    + ATTR_OUTPUT_PIPELINE + "' attribute");

        String source = resolveSource(streamId, attributes, event);
        List<Document> body = loadTemplateBody(streamId, pipelineName);
        Map<String, Object> baseVariables = EventVariables.build(attributes, event, RESERVED_ATTRS);
        List<Target> targets = RecomputeTargets.resolve(streamId, pipelineName,
                attributes != null ? attributes.get(ATTR_WRITE_STAGES) : null, body, logger);

        logger.info("Stream '{}' recompute: source='{}', pipeline='{}', targets={}, bodyStages={}",
                streamId, source, pipelineName, targets.size(), body.size());

        int ok = 0;
        for (Target target : targets)
            if (runTarget(streamId, source, body, baseVariables, target))
                ok++;
        logger.info("Stream '{}' recompute complete ({}/{} targets written)", streamId, ok, targets.size());
    }

    /**
     * The aggregation source: the {@link #ATTR_AGGREGATION_COLLECTION} override, or
     * the collection that produced the event.
     */
    private static String resolveSource(String streamId, Map<String, Object> attributes,
            ChangeStreamDocument<Document> event) {
        String override = attr(attributes, ATTR_AGGREGATION_COLLECTION);
        if (!override.isEmpty())
            return override;
        String eventCollection = event != null && event.getNamespace() != null
                ? event.getNamespace().getCollectionName() : null;
        if (eventCollection == null || eventCollection.isBlank())
            throw new IllegalStateException("Change stream '" + streamId + "' has no aggregation source: set the '"
                    + ATTR_AGGREGATION_COLLECTION + "' attribute or watch a specific collection");
        return eventCollection;
    }

    /**
     * Runs one target: the template body + this target's write stage, executed with
     * the shared variables overlaid by the target's own. Returns {@code true} on
     * success; a failure is logged (so one target can't abort the others).
     */
    private boolean runTarget(String streamId, String source, List<Document> body,
            Map<String, Object> baseVariables, Target target) {
        List<Document> stages = new ArrayList<>(body);
        if (target.writeStage() != null)
            stages.add(target.writeStage());
        Map<String, Object> variables = baseVariables;
        if (!target.variables().isEmpty()) {
            variables = new HashMap<>(baseVariables);
            variables.putAll(target.variables());
        }
        try {
            aggregationService.execute(AggregationSpec.of(source, stages), variables);
            return true;
        } catch (RuntimeException e) {
            logger.error("Stream '{}' target {} failed: {}", streamId, target.describe(), e.getMessage(), e);
            return false;
        }
    }

    /** Loads a template's stages (a mutable copy), without appending any write stage. */
    private List<Document> loadTemplateBody(String streamId, String pipelineName) {
        return pipelineRepository.findById(pipelineName)
                .map(template -> new ArrayList<>(template.getStages().stream().map(Document::new).toList()))
                .orElseThrow(() -> new IllegalStateException("Output pipeline '" + pipelineName
                        + "' configured on change stream '" + streamId + "' does not exist. Available templates: "
                        + availableTemplateNames()));
    }

    /** Best-effort list of known template names, for diagnostics only. */
    private String availableTemplateNames() {
        try {
            return pipelineRepository.findAll().stream().map(PipelineTemplate::getName).toList().toString();
        } catch (RuntimeException e) {
            return "<unavailable: " + e.getMessage() + ">";
        }
    }

    /** Reserved attribute keys the listener manages itself (not user placeholders). */
    private static final Set<String> RESERVED_ATTRS = Set.of(
            ATTR_OUTPUT_PIPELINE, ATTR_WRITE_STAGES, ATTR_AGGREGATION_COLLECTION,
            ATTR_RECOMPUTE_DEBOUNCE_MS, ATTR_RECOMPUTE_MAX_DELAY_MS);

    private static String attr(Map<String, Object> attrs, String key) {
        Object v = attrs != null ? attrs.get(key) : null;
        return v instanceof String s && !s.isBlank() ? s : "";
    }

    /**
     * Reads an attribute as a {@code long} (accepts a {@link Number} or a numeric
     * String). Returns {@code 0} when absent, blank or unparseable.
     */
    private static long longAttr(Map<String, Object> attrs, String key) {
        Object v = attrs != null ? attrs.get(key) : null;
        if (v instanceof Number n)
            return n.longValue();
        if (v instanceof String s && !s.isBlank()) {
            try {
                return Long.parseLong(s.trim());
            } catch (NumberFormatException ignored) {
                return 0L;
            }
        }
        return 0L;
    }
}
