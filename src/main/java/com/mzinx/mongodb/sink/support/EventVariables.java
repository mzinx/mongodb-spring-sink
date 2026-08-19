package com.mzinx.mongodb.sink.support;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;

/**
 * Builds the placeholder-variable map that parameterizes a materialized-view
 * recompute — the mapping from a change event + the stream's attributes to the
 * variables an aggregation pipeline can reference via {@code {"_ph": "..."}}.
 *
 * <ul>
 * <li>the change event's fields under {@code event.*} (e.g.
 * {@code {"_ph": "event.documentKey._id"}});</li>
 * <li>the stream's custom (non-reserved) attributes both at the top level
 * ({@code {"_ph": "tenantId"}}) and under {@code attr.*}
 * ({@code {"_ph": "attr.tenantId"}}).</li>
 * </ul>
 *
 * Values are supplied as {@link Document}/simple types so the aggregation layer's
 * variable binder can convert them and dotted-path placeholders can descend.
 */
public final class EventVariables {

    /** Top-level key exposing the change event's fields ({@code event.*}). */
    public static final String VAR_EVENT = "event";
    /** Top-level key exposing the stream's attributes ({@code attr.*}). */
    public static final String VAR_ATTR = "attr";

    private static final DocumentCodec DOCUMENT_CODEC = new DocumentCodec();

    private EventVariables() {
    }

    /**
     * @param attributes    the stream's free-form attributes (may be {@code null})
     * @param event         the triggering change event (may be {@code null})
     * @param reservedAttrs attribute keys the listener manages itself — excluded
     *                      from the top-level promotion (still available under
     *                      {@code attr.*})
     */
    public static Map<String, Object> build(Map<String, Object> attributes,
            ChangeStreamDocument<Document> event, Set<String> reservedAttrs) {
        Map<String, Object> vars = new HashMap<>();
        if (attributes != null)
            putAttributes(vars, attributes, reservedAttrs);
        if (event != null)
            vars.put(VAR_EVENT, eventDocument(event));
        return vars;
    }

    /** Exposes attributes under {@code attr.*}, promoting custom ones to the top level. */
    private static void putAttributes(Map<String, Object> vars, Map<String, Object> attributes,
            Set<String> reservedAttrs) {
        Document attrDoc = new Document();
        attributes.forEach(attrDoc::put);
        vars.put(VAR_ATTR, attrDoc);
        attributes.forEach((k, v) -> {
            if (k != null && !reservedAttrs.contains(k) && !VAR_EVENT.equals(k) && !VAR_ATTR.equals(k))
                vars.put(k, v);
        });
    }

    /** The change event's fields shaped into a {@code Document} for {@code event.*}. */
    private static Document eventDocument(ChangeStreamDocument<Document> event) {
        Document eventDoc = new Document();
        OperationType op = event.getOperationType();
        if (op != null)
            eventDoc.put("operationType", op.getValue());
        if (event.getNamespace() != null)
            eventDoc.put("ns", new Document()
                    .append("db", event.getNamespace().getDatabaseName())
                    .append("coll", event.getNamespace().getCollectionName()));
        putBson(eventDoc, "documentKey", event.getDocumentKey());
        // fullDocument is present for insert/replace, and for update when the stream
        // requested fullDocument lookup; fullDocumentBeforeChange when the before-image
        // is enabled.
        if (event.getFullDocument() != null)
            eventDoc.put("fullDocument", event.getFullDocument());
        if (event.getFullDocumentBeforeChange() != null)
            eventDoc.put("fullDocumentBeforeChange", event.getFullDocumentBeforeChange());
        return eventDoc;
    }

    /**
     * Stores a {@link BsonDocument} value (e.g. the event's {@code documentKey}) as a
     * {@link Document} so it nests cleanly and, once bound, preserves BSON types
     * (ObjectId, dates) rather than degrading to extended-JSON strings.
     */
    private static void putBson(Document target, String key, BsonDocument value) {
        if (value == null)
            return;
        target.put(key, DOCUMENT_CODEC.decode(value.asBsonReader(), DecoderContext.builder().build()));
    }
}
