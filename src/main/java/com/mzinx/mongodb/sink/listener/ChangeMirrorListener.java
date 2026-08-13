package com.mzinx.mongodb.sink.listener;

import java.util.Map;

import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mzinx.mongodb.changestream.listener.ChangeStreamListener;

/**
 * Lightweight, event-driven change-stream sink: mirrors each change event's
 * document into a destination collection with <em>O(1)</em> work per event.
 * <p>
 * Unlike the {@link MaterializedViewListener} — which re-runs an aggregation
 * over the <em>whole</em> source collection on every change (right for
 * {@code $group}/join rollups, but wasteful for simple mirroring) — this
 * listener writes only the single document that changed:
 * <ul>
 *   <li>{@code insert} / {@code replace} / {@code update} → upsert the event's
 *       {@code fullDocument} into the destination, keyed by {@code _id}.</li>
 *   <li>{@code delete} → remove {@code {_id: documentKey._id}} from the
 *       destination (unless {@link #ATTR_MIRROR_DELETE} is {@code "false"}).</li>
 * </ul>
 * <p>
 * <b>Filtering / reshaping is done by the change stream's own aggregation
 * pipeline</b> ({@code ChangeStreamConfig.pipeline}, i.e. the pipeline passed to
 * {@code collection.watch(pipeline)}), which runs over the stream of change
 * <em>events</em> — e.g. {@code $match} on {@code operationType} or a field, or
 * {@code $project} to reshape {@code fullDocument}. There is no output-pipeline
 * template and no terminal {@code $merge}/{@code $out}; nothing scans the source
 * collection.
 * <p>
 * Register a change stream with {@code listener = }{@value #BEAN_NAME} and an
 * {@code attributes.destination} naming the target collection.
 * <p>
 * <b>Updates and {@code fullDocument}:</b> an {@code update} event only carries a
 * {@code fullDocument} when the stream requests document lookup
 * ({@code fullDocument = UPDATE_LOOKUP} / {@code WHEN_AVAILABLE} /
 * {@code REQUIRED}). When it is absent this listener <em>skips</em> the event and
 * logs a warning hinting to enable lookup — it does not attempt a partial write.
 */
@Component(ChangeMirrorListener.BEAN_NAME)
public class ChangeMirrorListener implements ChangeStreamListener<Document> {

    public static final String BEAN_NAME = "changeMirrorListener";

    /** Attribute naming the destination collection to mirror events into (required). */
    public static final String ATTR_DESTINATION = "destination";

    /**
     * Optional attribute controlling whether {@code delete} events remove the
     * corresponding document from the destination. Defaults to {@code true}; set
     * to {@code "false"} for an append/upsert-only mirror that never deletes.
     */
    public static final String ATTR_MIRROR_DELETE = "mirrorDelete";

    private static final DocumentCodec DOCUMENT_CODEC = new DocumentCodec();
    private static final ReplaceOptions UPSERT = new ReplaceOptions().upsert(true);

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final MongoTemplate mongoTemplate;

    ChangeMirrorListener(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void onEvent(String streamId, Map<String, Object> attributes, ChangeStreamDocument<Document> event) {
        // Catch and log here so a misconfiguration surfaces with context and does
        // NOT tear down the change stream (which would stop future events).
        try {
            mirror(streamId, attributes, event);
        } catch (RuntimeException e) {
            logger.error("Mirror for stream '{}' failed: {}", streamId, e.getMessage(), e);
        }
    }

    private void mirror(String streamId, Map<String, Object> attributes, ChangeStreamDocument<Document> event) {
        if (event == null)
            return;

        String destination = attr(attributes, ATTR_DESTINATION);
        if (destination.isEmpty())
            throw new IllegalStateException("Change stream '" + streamId + "' is missing the required '"
                    + ATTR_DESTINATION + "' attribute");

        OperationType op = event.getOperationType();
        if (op == null) {
            logger.debug("Stream '{}' mirror: event with no operationType, ignored", streamId);
            return;
        }

        switch (op) {
            case INSERT, REPLACE, UPDATE -> upsert(streamId, destination, op, event);
            case DELETE -> delete(streamId, destination, attributes, event);
            default -> logger.debug("Stream '{}' mirror: operation '{}' not mirrored", streamId, op.getValue());
        }
    }

    /** Upserts the event's full document into the destination, keyed by {@code _id}. */
    private void upsert(String streamId, String destination, OperationType op, ChangeStreamDocument<Document> event) {
        Document fullDocument = event.getFullDocument();
        if (fullDocument == null) {
            // Updates need fullDocument lookup enabled on the stream; without it we
            // cannot mirror the current state, so skip rather than write partial data.
            logger.warn("Stream '{}' mirror: '{}' event has no fullDocument — skipping. "
                    + "Enable fullDocument lookup on the stream (UPDATE_LOOKUP) to mirror updates.",
                    streamId, op.getValue());
            return;
        }
        Object id = fullDocument.get("_id");
        if (id == null) {
            id = documentKeyId(event);
            if (id != null)
                fullDocument.put("_id", id);
        }
        if (id == null) {
            logger.warn("Stream '{}' mirror: '{}' event has no _id — skipping", streamId, op.getValue());
            return;
        }
        mongoTemplate.getCollection(destination)
                .replaceOne(new Document("_id", id), fullDocument, UPSERT);
        logger.debug("Stream '{}' mirror: upserted _id={} into '{}'", streamId, id, destination);
    }

    /** Removes the changed document from the destination (unless disabled). */
    private void delete(String streamId, String destination, Map<String, Object> attributes,
            ChangeStreamDocument<Document> event) {
        if ("false".equalsIgnoreCase(attr(attributes, ATTR_MIRROR_DELETE))) {
            logger.debug("Stream '{}' mirror: delete ignored ('{}' = false)", streamId, ATTR_MIRROR_DELETE);
            return;
        }
        Object id = documentKeyId(event);
        if (id == null) {
            logger.warn("Stream '{}' mirror: delete event has no documentKey._id — skipping", streamId);
            return;
        }
        mongoTemplate.getCollection(destination).deleteOne(new Document("_id", id));
        logger.debug("Stream '{}' mirror: deleted _id={} from '{}'", streamId, id, destination);
    }

    /**
     * Reads {@code documentKey._id} from the event, decoded to a plain Java/BSON
     * value (e.g. {@link org.bson.types.ObjectId}) so it matches destination docs.
     */
    private static Object documentKeyId(ChangeStreamDocument<Document> event) {
        if (event.getDocumentKey() == null)
            return null;
        Document key = DOCUMENT_CODEC.decode(event.getDocumentKey().asBsonReader(), DecoderContext.builder().build());
        Object id = key.get("_id");
        // Guard against a raw BsonValue slipping through (shouldn't, after decode).
        return id instanceof BsonValue ? null : id;
    }

    private static String attr(Map<String, Object> attrs, String key) {
        Object v = attrs != null ? attrs.get(key) : null;
        return v instanceof String s && !s.isBlank() ? s : "";
    }
}
