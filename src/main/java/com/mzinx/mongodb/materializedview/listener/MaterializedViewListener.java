package com.mzinx.mongodb.materializedview.listener;

import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mzinx.mongodb.aggregation.dao.PipelineRepository;
import com.mzinx.mongodb.aggregation.model.AggregationSpec;
import com.mzinx.mongodb.aggregation.model.PipelineTemplate;
import com.mzinx.mongodb.aggregation.service.AggregationService;
import com.mzinx.mongodb.changestream.listener.ChangeStreamListener;
import com.mzinx.mongodb.materializedview.model.MaterializedViewRecomputedEvent;

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
 * After each recompute a {@link MaterializedViewRecomputedEvent} is published so
 * other components (e.g. a WebSocket/messaging layer) can react — e.g. broadcast
 * a refresh hint to live clients — without this module depending on them.
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

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final PipelineRepository pipelineRepository;
    private final AggregationService aggregationService;
    private final ApplicationEventPublisher eventPublisher;

    MaterializedViewListener(PipelineRepository pipelineRepository, AggregationService aggregationService,
            ApplicationEventPublisher eventPublisher) {
        this.pipelineRepository = pipelineRepository;
        this.aggregationService = aggregationService;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public void onEvent(String streamId, Map<String, Object> attributes, ChangeStreamDocument<Document> event) {
        logger.info("Source change detected on stream '{}' ({}), recomputing view",
                streamId, event.getOperationType() != null ? event.getOperationType().getValue() : "?");
        // Use the attributes delivered with the event — no per-event config lookup.
        recompute(event.getNamespace().getCollectionName(), streamId, attributes);
    }

    /**
     * Recomputes the whole output collection for the given change stream using the
     * supplied attributes (delivered with the change stream event, or passed by a
     * direct caller), then publishes a {@link MaterializedViewRecomputedEvent}.
     *
     * @throws IllegalStateException if the stream has no {@code outputPipeline}
     *                               attribute, or it names a pipeline that does not
     *                               exist — both are misconfiguration and must be
     *                               fixed before the listener can run.
     */
    public synchronized void recompute(String sourceCollection, String streamId, Map<String, Object> attributes) {
        String pipelineName = attr(attributes, ATTR_OUTPUT_PIPELINE);
        if (pipelineName.isEmpty())
            throw new IllegalStateException("Change stream '" + streamId + "' is missing the required '"
                    + ATTR_OUTPUT_PIPELINE + "' attribute");

        List<Document> stages = pipelineRepository.findById(pipelineName)
                .map(template -> template.getStages().stream().map(Document::new).toList())
                .orElseThrow(() -> new IllegalStateException("Output pipeline '" + pipelineName
                        + "' configured on change stream '" + streamId + "' does not exist"));

        // Run the recompute: read the source collection and let the pipeline's
        // terminal $merge/$out write the view.
        aggregationService.execute(AggregationSpec.of(sourceCollection, stages));

        // Notify any interested components (decoupled extension seam).
        eventPublisher.publishEvent(new MaterializedViewRecomputedEvent(this, sourceCollection, streamId));
    }

    private static String attr(Map<String, Object> attrs, String key) {
        Object v = attrs != null ? attrs.get(key) : null;
        return v instanceof String s && !s.isBlank() ? s : "";
    }
}
