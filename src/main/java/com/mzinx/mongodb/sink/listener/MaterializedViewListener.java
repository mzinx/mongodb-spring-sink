package com.mzinx.mongodb.sink.listener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mzinx.mongodb.aggregation.dao.PipelineRepository;
import com.mzinx.mongodb.aggregation.model.AggregationSpec;
import com.mzinx.mongodb.aggregation.model.PipelineTemplate;
import com.mzinx.mongodb.aggregation.service.AggregationService;
import com.mzinx.mongodb.changestream.listener.ChangeStreamListener;

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
 */
@Component(MaterializedViewListener.BEAN_NAME)
public class MaterializedViewListener implements ChangeStreamListener<Document> {

    public static final String BEAN_NAME = "materializedViewListener";

    /** Attribute key naming the output pipeline to run (on the change stream config). */
    public static final String ATTR_OUTPUT_PIPELINE = "outputPipeline";

    /**
     * Optional attribute holding the terminal write stage to append at runtime.
     * <p>
     * This lets a shared, write-stage-free {@link PipelineTemplate} be reused by
     * many streams, each writing to its own target: the template supplies the
     * transformation body and each stream carries its own {@code $merge}/{@code $out}
     * configuration here. The value is the write stage verbatim, e.g.
     * <pre>{"$merge": {"into": "orders_view", "whenMatched": "replace", "whenNotMatched": "insert"}}</pre>
     * or <pre>{"$out": "orders_snapshot"}</pre>.
     * <p>
     * When present it is appended after the template's stages — unless the
     * template already ends in its own {@code $merge}/{@code $out}, in which case
     * the template's own write stage wins and this attribute is ignored (so
     * self-contained templates keep working).
     */
    public static final String ATTR_WRITE_STAGE = "writeStage";

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
     * Prefix under which the change event's fields are exposed as pipeline
     * placeholder variables. A pipeline can reference them with dotted paths, e.g.
     * <pre>{"_ph": "event.documentKey._id"}</pre>,
     * <pre>{"_ph": "event.fullDocument.status"}</pre> or
     * <pre>{"_ph": "event.operationType"}</pre>. The available sub-fields are
     * {@code operationType}, {@code documentKey}, {@code ns} (with {@code db} /
     * {@code coll}), {@code fullDocument} and {@code fullDocumentBeforeChange}.
     */
    public static final String VAR_EVENT = "event";

    /**
     * Prefix under which the change stream config's custom {@code attributes} are
     * exposed as pipeline placeholder variables, e.g.
     * <pre>{"_ph": "attr.tenantId"}</pre>. Every entry in the stream's
     * {@code attributes} map is available here in addition to the reserved keys
     * ({@code outputPipeline}, {@code writeStage}, {@code aggregationCollection}).
     */
    public static final String VAR_ATTR = "attr";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final PipelineRepository pipelineRepository;
    private final AggregationService aggregationService;

    MaterializedViewListener(PipelineRepository pipelineRepository, AggregationService aggregationService) {
        this.pipelineRepository = pipelineRepository;
        this.aggregationService = aggregationService;
    }

    @Override
    public void onEvent(String streamId, Map<String, Object> attributes, ChangeStreamDocument<Document> event) {
        logger.info("Source change detected on stream '{}' ({}), recomputing view. Attribute keys={}",
                streamId, event.getOperationType() != null ? event.getOperationType().getValue() : "?",
                attributes != null ? attributes.keySet() : null);
        // Use the attributes delivered with the event — no per-event config lookup.
        // Catch and log here so a misconfiguration surfaces with full context and
        // does NOT tear down the change stream (which would stop future events).
        try {
            recompute(streamId, attributes, event);
        } catch (RuntimeException e) {
            logger.error("Recompute for stream '{}' failed: {}", streamId, e.getMessage(), e);
        }
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

        List<Document> stages = resolveStages(streamId, pipelineName, attributes);

        // The aggregation source: an explicit override, or the event's collection.
        String eventCollection = event != null && event.getNamespace() != null
                ? event.getNamespace().getCollectionName()
                : null;
        String aggregationCollection = attr(attributes, ATTR_AGGREGATION_COLLECTION);
        String sourceCollection = !aggregationCollection.isEmpty() ? aggregationCollection : eventCollection;
        if (sourceCollection == null || sourceCollection.isBlank())
            throw new IllegalStateException("Change stream '" + streamId + "' has no aggregation source: set the '"
                    + ATTR_AGGREGATION_COLLECTION + "' attribute or watch a specific collection");

        // Expose the change event and the stream's custom attributes as pipeline
        // placeholder variables so the pipeline can be parameterized per event.
        Map<String, Object> variables = buildVariables(attributes, event);
        logger.info("Stream '{}' recompute: source='{}', pipeline='{}', variableKeys={}, stageCount={}",
                streamId, sourceCollection, pipelineName, variables.keySet(), stages.size());
        if (logger.isDebugEnabled())
            logger.debug("Stream '{}' stages before bind={}", streamId, stages);

        // Run the recompute: read the source collection and let the pipeline's
        // terminal $merge/$out write the view.
        aggregationService.execute(AggregationSpec.of(sourceCollection, stages), variables);
        logger.info("Stream '{}' recompute complete", streamId);
    }

    /**
     * Loads the named template's stages and, when the stream defines a
     * {@link #ATTR_WRITE_STAGE} attribute, appends that write stage so a shared,
     * write-stage-free template can be reused across streams that each write to a
     * different target. If the template already ends in its own
     * {@code $merge}/{@code $out}, that write stage is kept and the attribute is
     * ignored.
     */
    private List<Document> resolveStages(String streamId, String pipelineName, Map<String, Object> attributes) {
        List<Document> stages = pipelineRepository.findById(pipelineName)
                .map(template -> new ArrayList<>(template.getStages().stream().map(Document::new).toList()))
                .orElseThrow(() -> new IllegalStateException("Output pipeline '" + pipelineName
                        + "' configured on change stream '" + streamId + "' does not exist. Available templates: "
                        + availableTemplateNames()));

        Document writeStage = writeStage(attributes);
        if (writeStage == null) {
            if (!endsWithWriteStage(stages))
                logger.warn("Stream '{}' template '{}' has no terminal $merge/$out and no '{}' attribute — "
                        + "the recompute will produce no output (nothing is written)",
                        streamId, pipelineName, ATTR_WRITE_STAGE);
            return stages;
        }
        if (endsWithWriteStage(stages)) {
            logger.debug("Stream '{}' template '{}' already ends in a write stage; ignoring '{}' attribute",
                    streamId, pipelineName, ATTR_WRITE_STAGE);
            return stages;
        }
        stages.add(writeStage);
        return stages;
    }

    /** Best-effort list of known template names, for diagnostics only. */
    private String availableTemplateNames() {
        try {
            return pipelineRepository.findAll().stream().map(PipelineTemplate::getName).toList().toString();
        } catch (RuntimeException e) {
            return "<unavailable: " + e.getMessage() + ">";
        }
    }

    /** True when the last stage of the pipeline is a {@code $merge} or {@code $out}. */
    private static boolean endsWithWriteStage(List<Document> stages) {
        if (stages == null || stages.isEmpty())
            return false;
        Document last = stages.get(stages.size() - 1);
        return last != null && (last.containsKey("$merge") || last.containsKey("$out"));
    }

    /**
     * Reads the {@link #ATTR_WRITE_STAGE} attribute as a single-stage
     * {@link Document} (e.g. {@code {"$merge": {...}}} or {@code {"$out": "coll"}}),
     * or {@code null} when it isn't set. Accepts either a {@code Map} (from JSON
     * config) or an already-parsed {@code Document}.
     */
    @SuppressWarnings("unchecked")
    private static Document writeStage(Map<String, Object> attrs) {
        Object v = attrs != null ? attrs.get(ATTR_WRITE_STAGE) : null;
        if (v instanceof Document d)
            return d.isEmpty() ? null : d;
        if (v instanceof Map<?, ?> m)
            return m.isEmpty() ? null : new Document((Map<String, Object>) m);
        return null;
    }

    /** Reserved attribute keys the listener manages itself (not user placeholders). */
    private static final java.util.Set<String> RESERVED_ATTRS = java.util.Set.of(
            ATTR_OUTPUT_PIPELINE, ATTR_WRITE_STAGE, ATTR_AGGREGATION_COLLECTION);

    /**
     * Builds the placeholder-variable map. Custom (non-reserved) attributes are
     * exposed BOTH at the top level (so {@code {"_ph": "tenantId"}} works) and
     * under {@code attr.*} (so {@code {"_ph": "attr.tenantId"}} works). The change
     * event's fields are exposed under {@code event.*}. Values are supplied as
     * {@link Document}/simple types so {@link AggregationSpec#bindVariables} can
     * convert them and dotted-path placeholders can descend into them.
     */
    private static Map<String, Object> buildVariables(Map<String, Object> attributes,
            ChangeStreamDocument<Document> event) {
        Map<String, Object> vars = new HashMap<>();

        if (attributes != null) {
            // Full attributes (incl. reserved keys) available under attr.*
            Document attrDoc = new Document();
            attributes.forEach(attrDoc::put);
            vars.put(VAR_ATTR, attrDoc);
            // Custom attributes also promoted to the top level for convenience.
            attributes.forEach((k, v) -> {
                if (k != null && !RESERVED_ATTRS.contains(k) && !VAR_EVENT.equals(k) && !VAR_ATTR.equals(k))
                    vars.put(k, v);
            });
        }

        if (event != null) {
            Document eventDoc = new Document();
            OperationType op = event.getOperationType();
            if (op != null)
                eventDoc.put("operationType", op.getValue());
            if (event.getNamespace() != null) {
                eventDoc.put("ns", new Document()
                        .append("db", event.getNamespace().getDatabaseName())
                        .append("coll", event.getNamespace().getCollectionName()));
            }
            putBson(eventDoc, "documentKey", event.getDocumentKey());
            // fullDocument is present for insert/replace, and for update when the
            // stream requested fullDocument lookup (updateLookup / whenAvailable /
            // required); fullDocumentBeforeChange when before-image is enabled.
            if (event.getFullDocument() != null)
                eventDoc.put("fullDocument", event.getFullDocument());
            if (event.getFullDocumentBeforeChange() != null)
                eventDoc.put("fullDocumentBeforeChange", event.getFullDocumentBeforeChange());
            vars.put(VAR_EVENT, eventDoc);
        }

        return vars;
    }

    private static final DocumentCodec DOCUMENT_CODEC = new DocumentCodec();

    /**
     * Stores a {@link BsonDocument} value (e.g. the change event's
     * {@code documentKey}) as a {@link Document} so it nests cleanly and, once
     * bound, preserves BSON types (ObjectId, dates) rather than degrading to
     * extended-JSON strings.
     */
    private static void putBson(Document target, String key, BsonDocument value) {
        if (value == null)
            return;
        target.put(key, DOCUMENT_CODEC.decode(value.asBsonReader(), DecoderContext.builder().build()));
    }

    private static String attr(Map<String, Object> attrs, String key) {
        Object v = attrs != null ? attrs.get(key) : null;
        return v instanceof String s && !s.isBlank() ? s : "";
    }
}
