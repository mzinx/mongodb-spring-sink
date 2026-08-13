# mongodb-spring-sink

A small Spring Boot library of **change-stream-driven collection sinks** for
MongoDB. On every change to a watched source collection it writes to a
destination collection. It ships two generic `ChangeStreamListener` beans:

- **`materializedViewListener`** — re-runs a configured aggregation pipeline
  (whose terminal `$merge`/`$out` writes the result) over the source collection.
  Right for rollups/joins (`$group`, `$lookup`) that must see all documents.
- **`changeMirrorListener`** — writes only the **changed document** into a
  destination (upsert by `_id`, delete on delete), `O(1)` per event. Right for
  filtering/reshaping/mirroring individual changes; filtering is done by the
  change stream's own `watch()` pipeline, not by scanning the collection.

It builds on:

- [`mongodb-spring-change-stream`](https://github.com/mzinx/mongodb-spring-change-stream)
  — drives the listeners from a persisted `ChangeStreamConfig`.
- [`mongodb-spring-aggregation`](https://github.com/mzinx/mongodb-spring-aggregation)
  (transitive) — runs the pipeline templates stored in `_pipelines` (used by the
  materialized-view listener).

## Choosing a listener

| | `materializedViewListener` | `changeMirrorListener` |
| --- | --- | --- |
| Per-event cost | `O(source size)` — aggregates the whole collection | `O(1)` — writes one document |
| Filtering | output-pipeline `$match` (still scans) | the change stream's `watch()` pipeline |
| Writes | pipeline's terminal `$merge`/`$out` | upsert changed doc by `_id`; delete on delete |
| Use when | `$group`/joins/rollups need all docs | mirror/filter/reshape individual changes |

## Materialized view

1. Save a pipeline template (in `_pipelines`) whose final stage is a `$merge`
   into your view collection.
2. Register a change stream whose `listener` is `materializedViewListener` and
   whose `attributes.outputPipeline` names that template:

```java
changeStreamConfigService.save(ChangeStreamConfig.builder()
        .id("order-summary")
        .collectionName("orders")
        .mode(Mode.AUTO_RECOVER)
        .listener(MaterializedViewListener.BEAN_NAME) // "materializedViewListener"
        .attributes(Map.of(MaterializedViewListener.ATTR_OUTPUT_PIPELINE, "orders-daily-summary"))
        .enabled(true)
        .build());
```

The change-stream library resolves the bean by name and invokes it per event;
the listener recomputes the whole view keyed by `_id` via the pipeline's terminal
`$merge`/`$out`. The recompute is authoritative — the `$merge`/`$out` has written
the view by the time `onEvent` returns.

> **Notifying live clients.** If you need to tell WebSocket clients a view
> changed, don't do it from this listener — the recompute's own `$merge`/`$out`
> write is itself a change-stream event on the output collection, so watch that
> collection (e.g. via `mongodb-spring-message-queuing`'s `messaging.watch-collections`)
> or broadcast a refresh at the point that triggered the write. This keeps the
> sink module free of any messaging/refresh coupling.

## Event-driven mirror

Register a change stream whose `listener` is `changeMirrorListener` and whose
`attributes.destination` names the target collection. Do any filtering/reshaping
in the stream's own aggregation `pipeline` (passed to `collection.watch(...)`),
which runs over the stream of change **events**:

```java
changeStreamConfigService.save(ChangeStreamConfig.builder()
        .id("orders-mirror")
        .collectionName("orders")
        // Filter the EVENTS (not the collection): only inserts of paid orders.
        .pipeline(List.of(new Document("$match", new Document("operationType", "insert")
                .append("fullDocument.status", "paid"))))
        // Needed to mirror UPDATE events (they carry fullDocument only on lookup).
        .fullDocument(FullDocument.UPDATE_LOOKUP)
        .listener(ChangeMirrorListener.BEAN_NAME) // "changeMirrorListener"
        .attributes(Map.of(ChangeMirrorListener.ATTR_DESTINATION, "orders_paid"))
        .enabled(true)
        .build());
```

Per event the listener:

- `insert` / `replace` / `update` → upsert `event.fullDocument` into the
  destination, keyed by `_id`.
- `delete` → remove `{_id: documentKey._id}` from the destination, unless
  `attributes.mirrorDelete = "false"`.

> **Updates need `fullDocument` lookup.** An `update` event only carries a
> `fullDocument` when the stream sets `fullDocument = UPDATE_LOOKUP` (or
> `WHEN_AVAILABLE`/`REQUIRED`). Without it, update events are skipped with a
> warning.

### Mirror attributes

| Attribute      | Required | Default | Purpose                                         |
| -------------- | -------- | ------- | ----------------------------------------------- |
| `destination`  | yes      | —       | Target collection to mirror changed docs into   |
| `mirrorDelete` | no       | `true`  | Whether `delete` events remove from destination |

## Configuration

| Property        | Default | Purpose                           |
| --------------- | ------- | --------------------------------- |
| `sink.enabled`  | `true`  | Master on/off for the auto-config |

## Requirements

- Java 21+ and Maven
- A MongoDB replica set / Atlas (change streams require it)
