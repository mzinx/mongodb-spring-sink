# mongodb-spring-materialized-view

A small Spring Boot library that maintains **change-stream-driven materialized
views** on MongoDB. It ships one generic `ChangeStreamListener` bean,
`materializedViewListener`, that — on every change to a watched source
collection — re-runs a configured aggregation pipeline whose terminal `$merge`
(or `$out`) writes the result into an output collection.

It builds on:

- [`mongodb-spring-change-stream`](https://github.com/mzinx/mongodb-spring-change-stream)
  — drives the listener from a persisted `ChangeStreamConfig`.
- [`mongodb-spring-aggregation`](https://github.com/mzinx/mongodb-spring-aggregation)
  (transitive) — runs the pipeline templates stored in `_pipelines`.

## How it works

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
`$merge`/`$out`.

## Extension seam: `MaterializedViewRecomputedEvent`

After each recompute the listener publishes a
`MaterializedViewRecomputedEvent(sourceCollection, streamId)`. Other components
can react **without this module depending on them** — for example, a
WebSocket/messaging layer can broadcast a refresh hint to live clients:

```java
@Component
class ViewRefreshBroadcaster {
    private final MessageService messageService;
    private final CommandMessages commandMessages;
    // ...
    @EventListener
    void onRecomputed(MaterializedViewRecomputedEvent e) {
        messageService.broadcast(commandMessages.refresh(e.getSourceCollection()));
    }
}
```

The recompute is authoritative (the `$merge` has already written the view); the
event is only a notification.

## Configuration

| Property                    | Default | Purpose                          |
| --------------------------- | ------- | -------------------------------- |
| `materialized-view.enabled` | `true`  | Master on/off for the auto-config |

## Requirements

- Java 21+ and Maven
- A MongoDB replica set / Atlas (change streams require it)
