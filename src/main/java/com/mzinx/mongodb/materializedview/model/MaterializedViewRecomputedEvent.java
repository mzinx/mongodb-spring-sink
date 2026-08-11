package com.mzinx.mongodb.materializedview.model;

import org.springframework.context.ApplicationEvent;

/**
 * Published after a materialized view has been recomputed, so other components
 * can react without the materialized-view module depending on them.
 * <p>
 * This is the module's extension seam: e.g. a WebSocket/messaging layer can
 * {@code @EventListener} this and broadcast a "refresh" hint to live clients.
 * The recompute itself is authoritative (the pipeline's terminal {@code $merge}
 * has already written the view); this event is only a notification.
 * <p>
 * The event's {@link #getSource() source} is the listener/component that
 * produced it (per {@link ApplicationEvent}); the meaningful payload is
 * {@link #getSourceCollection()} and {@link #getStreamId()}.
 */
public class MaterializedViewRecomputedEvent extends ApplicationEvent {

    private final String sourceCollection;
    private final String streamId;

    /**
     * @param source           the component publishing the event (never {@code null})
     * @param sourceCollection the collection whose change triggered the recompute
     * @param streamId         the id of the change stream that drove the recompute
     */
    public MaterializedViewRecomputedEvent(Object source, String sourceCollection, String streamId) {
        super(source);
        this.sourceCollection = sourceCollection;
        this.streamId = streamId;
    }

    /** The source collection that was aggregated to (re)build the view. */
    public String getSourceCollection() {
        return sourceCollection;
    }

    /** The change stream id that triggered (or requested) the recompute. */
    public String getStreamId() {
        return streamId;
    }
}
