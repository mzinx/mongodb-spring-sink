package com.mzinx.mongodb.sink.support;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.model.changestream.ChangeStreamDocument;

/**
 * Coalesces (debounces) full-recompute requests <b>per stream</b>, so a burst of
 * change events collapses into a single recompute instead of one per event.
 *
 * <p>This is the mitigation for the "repeated calculation" problem: e.g. inserting
 * ten orders fires ten change events; without coalescing that runs the same
 * full-collection {@code $group} rollup ten times, when only the final result
 * matters. A full recompute is idempotent and latest-wins, so dropping the
 * intermediate runs loses nothing — the final run reflects every event in the
 * window.
 *
 * <h2>Semantics</h2>
 * Keyed by {@code streamId}:
 * <ul>
 * <li>Each event records the <em>latest</em> pending context and (re)arms a timer
 * for {@code debounceMs} of quiet; when it fires, exactly one recompute runs.</li>
 * <li>{@code maxDelayMs} (optional) caps how long a continuously-busy stream can be
 * held back, so a steady event rate still flushes at least that often (prevents
 * starvation). {@code 0} disables the cap.</li>
 * <li><b>Single-flight:</b> if events arrive while a recompute is running, the
 * stream is marked dirty and exactly one follow-up recompute is scheduled after it
 * finishes.</li>
 * </ul>
 *
 * <h2>Trade-off (must be understood before enabling)</h2>
 * Coalescing returns control to the change-stream loop <em>before</em> the
 * recompute runs, so the resume token can advance ahead of the (deferred) write.
 * A crash inside the debounce window can therefore drop a pending recompute — the
 * output is eventually-consistent, healed by the next event (or a restart
 * recompute). This is an acceptable trade for idempotent full recomputes and is
 * why coalescing is <b>opt-in per stream</b>. Do NOT enable it for per-document /
 * scoped recomputes (those are O(1) and each event is distinct work) — leave those
 * on the immediate path.
 */
public final class RecomputeCoalescer {

    private static final Logger logger = LoggerFactory.getLogger(RecomputeCoalescer.class);

    /** The recompute to run once per coalesced window (the listener's core work). */
    @FunctionalInterface
    public interface RecomputeTask {
        void run(String streamId, Map<String, Object> attributes, ChangeStreamDocument<Document> event);
    }

    /** Mutable per-stream coalescing state, guarded by its own monitor. */
    private static final class Pending {
        Map<String, Object> attributes;
        ChangeStreamDocument<Document> event;
        ScheduledFuture<?> timer;
        boolean running;
        boolean dirtyWhileRunning;
        long firstRequestAtMs; // when the current (un-flushed) window opened
    }

    private final ScheduledExecutorService scheduler;
    private final RecomputeTask task;
    private final ConcurrentHashMap<String, Pending> pending = new ConcurrentHashMap<>();

    public RecomputeCoalescer(RecomputeTask task) {
        this.task = task;
        AtomicLong n = new AtomicLong();
        this.scheduler = Executors.newScheduledThreadPool(
                Math.max(2, Runtime.getRuntime().availableProcessors() / 2), r -> {
                    Thread t = new Thread(r, "sink-recompute-" + n.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                });
    }

    /**
     * Records a recompute request for {@code streamId} and (re)arms its debounce
     * timer. The most recent {@code attributes}/{@code event} win as the context
     * for the eventual run (a full recompute rescans the source regardless, so the
     * triggering event only needs to be a representative one).
     */
    public void submit(String streamId, Map<String, Object> attributes, ChangeStreamDocument<Document> event,
            long debounceMs, long maxDelayMs) {
        Pending p = pending.computeIfAbsent(streamId, k -> new Pending());
        synchronized (p) {
            p.attributes = attributes;
            p.event = event;
            if (p.firstRequestAtMs == 0L)
                p.firstRequestAtMs = System.currentTimeMillis();

            // If we're past the max-delay cap, flush now rather than re-arming.
            long waited = System.currentTimeMillis() - p.firstRequestAtMs;
            long delay = debounceMs;
            if (maxDelayMs > 0) {
                long remainingCap = maxDelayMs - waited;
                if (remainingCap <= 0) {
                    fire(streamId);
                    return;
                }
                delay = Math.min(debounceMs, remainingCap);
            }

            if (p.timer != null)
                p.timer.cancel(false);
            p.timer = scheduler.schedule(() -> fire(streamId), delay, TimeUnit.MILLISECONDS);
        }
    }

    /** Runs (or defers, if already running) the coalesced recompute for a stream. */
    private void fire(String streamId) {
        Pending p = pending.get(streamId);
        if (p == null)
            return;
        Map<String, Object> attributes;
        ChangeStreamDocument<Document> event;
        synchronized (p) {
            if (p.running) {
                // A run is in flight — mark dirty; the finishing run reschedules.
                p.dirtyWhileRunning = true;
                return;
            }
            p.running = true;
            p.timer = null;
            p.firstRequestAtMs = 0L;
            attributes = p.attributes;
            event = p.event;
        }

        try {
            task.run(streamId, attributes, event);
        } catch (RuntimeException e) {
            logger.error("Coalesced recompute for stream '{}' failed: {}", streamId, e.getMessage(), e);
        } finally {
            boolean rerun;
            synchronized (p) {
                p.running = false;
                rerun = p.dirtyWhileRunning;
                p.dirtyWhileRunning = false;
            }
            // Events arrived during the run: schedule exactly one immediate follow-up.
            if (rerun)
                scheduler.schedule(() -> fire(streamId), 0, TimeUnit.MILLISECONDS);
        }
    }

    /** Stops the scheduler (best-effort). Pending recomputes are abandoned. */
    public void shutdown() {
        scheduler.shutdownNow();
    }
}
