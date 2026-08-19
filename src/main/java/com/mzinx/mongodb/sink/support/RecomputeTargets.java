package com.mzinx.mongodb.sink.support;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.slf4j.Logger;

/**
 * Resolves the fan-out <b>targets</b> of a materialized-view recompute from the
 * stream's {@code writeStages} attribute — one {@link Target} per collection the
 * same recompute writes to.
 *
 * <p>A target is a terminal write stage ({@code $merge}/{@code $out}) plus optional
 * per-target placeholder variables. When {@code writeStages} is absent the template
 * is expected to be <em>self-contained</em> (already ends in its own write stage),
 * which yields a single target with no appended write stage.
 */
public final class RecomputeTargets {

    private RecomputeTargets() {
    }

    /**
     * A single fan-out target: extra placeholder {@code variables} + its own
     * {@code writeStage} ({@code null} for a self-contained template). {@code variables}
     * is never {@code null} (an empty map is substituted).
     */
    public record Target(Map<String, Object> variables, Document writeStage) {
        public Target {
            variables = variables != null ? variables : Map.of();
        }

        /** Short human label for logs, e.g. {@code "$merge->ordersByDay"}. */
        public String describe() {
            if (writeStage == null)
                return "(template write stage)";
            Object merge = writeStage.get("$merge");
            if (merge instanceof Document m)
                return "$merge->" + m.get("into");
            if (merge != null)
                return "$merge->" + merge;
            Object out = writeStage.get("$out");
            return out != null ? "$out->" + out : writeStage.toString();
        }
    }

    /**
     * Resolves the targets for a stream:
     * <ul>
     * <li>if {@code writeStagesAttr} is a non-empty list, one {@link Target} per
     * entry (each with its own {@code writeStage} and optional {@code variables});
     * a single target is just a one-element list;</li>
     * <li>otherwise a single target with no write stage — valid only when
     * {@code body} is self-contained (already ends in {@code $merge}/{@code $out}),
     * else a warning is logged and the recompute produces no output.</li>
     * </ul>
     *
     * @param writeStagesAttr the {@code writeStages} attribute value (any type)
     * @param body            the template body (to check for a self-contained write stage)
     */
    @SuppressWarnings("unchecked")
    public static List<Target> resolve(String streamId, String pipelineName, Object writeStagesAttr,
            List<Document> body, Logger logger) {
        if (writeStagesAttr instanceof List<?> entries && !entries.isEmpty()) {
            List<Target> targets = new ArrayList<>(entries.size());
            for (Object raw : entries) {
                if (!(raw instanceof Map<?, ?> entry)) {
                    logger.warn("Stream '{}' writeStages entry is not a map, skipped: {}", streamId, raw);
                    continue;
                }
                Document ws = asDocument(entry.get("writeStage"));
                if (ws == null) {
                    logger.warn("Stream '{}' writeStages entry has no 'writeStage', skipped: {}", streamId, entry);
                    continue;
                }
                Map<String, Object> vars = entry.get("variables") instanceof Map<?, ?> m
                        ? (Map<String, Object>) m : Map.of();
                targets.add(new Target(vars, ws));
            }
            if (targets.isEmpty())
                throw new IllegalStateException(
                        "Change stream '" + streamId + "' writeStages had no usable entries");
            return targets;
        }

        // No writeStages: the template must be self-contained (end in its own write stage).
        if (!endsWithWriteStage(body))
            logger.warn("Stream '{}' template '{}' has no terminal $merge/$out and no writeStages attribute — "
                    + "the recompute will produce no output (nothing is written)", streamId, pipelineName);
        return List.of(new Target(Map.of(), null));
    }

    /** True when the last stage of the pipeline is a {@code $merge} or {@code $out}. */
    public static boolean endsWithWriteStage(List<Document> stages) {
        if (stages == null || stages.isEmpty())
            return false;
        Document last = stages.get(stages.size() - 1);
        return last != null && (last.containsKey("$merge") || last.containsKey("$out"));
    }

    /**
     * Coerces a value into a non-empty {@link Document}, or {@code null}. Accepts an
     * already-parsed {@code Document} or a {@code Map} (from JSON config).
     */
    @SuppressWarnings("unchecked")
    private static Document asDocument(Object v) {
        if (v instanceof Document d)
            return d.isEmpty() ? null : d;
        if (v instanceof Map<?, ?> m)
            return m.isEmpty() ? null : new Document((Map<String, Object>) m);
        return null;
    }
}
