package com.aerospike.examples.hotkeys.helper;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Operation;
import com.aerospike.client.sdk.OperationResult;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.Session;

/**
 * SDK port of the legacy {@code HotKeyReducer} (see ../../../java's {@code hotkeys.helper}
 * package - vendored from the standalone hot-key-reducer project). Batches concurrent operate
 * calls against the same hot key within the JVM into a single Aerospike operate call, then
 * unpacks the combined result back to each caller.
 * <p/>
 * The legacy version reconstructs each caller's slice of the combined result by counting how many
 * times each bin name appears across the whole batch (a bin touched once returns a scalar; touched
 * more than once returns a list, so unpacking needs per-bin-name index bookkeeping). This SDK
 * exposes {@link Record#results}, an array of per-operation results in submission order - so each
 * caller's slice of a merged batch is just an index range of that array, no bin-name counting
 * needed. This is a genuine simplification the new SDK enables, not a workaround.
 */
public class HotKeyReducer {

    private static class SubmittedOps {
        private final CompletableFuture<Record> future;
        private final int startIndex;
        private final int opCount;

        SubmittedOps(CompletableFuture<Record> future, int startIndex, int opCount) {
            this.future = future;
            this.startIndex = startIndex;
            this.opCount = opCount;
        }

        void finish(Record aggregated) {
            if (aggregated == null) {
                future.complete(null);
                return;
            }
            OperationResult[] slice = Arrays.copyOfRange(aggregated.results, startIndex, startIndex + opCount);
            future.complete(new Record(Map.of(), slice, aggregated.generation, aggregated.expiration));
        }

        void fail(Exception e) {
            future.completeExceptionally(e);
        }
    }

    private static class OperationBatch {
        private final List<Operation> operations = new ArrayList<>();
        private final List<SubmittedOps> operationCalls = new ArrayList<>();

        void submitOperationCall(CompletableFuture<Record> future, Operation... ops) {
            int startIndex = operations.size();
            operations.addAll(Arrays.asList(ops));
            operationCalls.add(new SubmittedOps(future, startIndex, ops.length));
        }

        Operation[] getOperations() {
            return operations.toArray(new Operation[0]);
        }

        boolean isSingleOperation() {
            return operationCalls.size() == 1;
        }

        void finish(Record results) {
            operationCalls.forEach(call -> call.finish(results));
        }

        void setFuturesWithException(Exception e) {
            operationCalls.forEach(call -> call.fail(e));
        }
    }

    private static class Stats {
        private final AtomicLong expiry = new AtomicLong(0);
        private final AtomicLong counter = new AtomicLong(0);
    }

    /** Monitors key access patterns to determine when the reducer should batch a key's operations. */
    private class ReducerMonitor {
        private Map<Key, Stats> currentStatsMap = new HashMap<>();
        private Map<Key, Stats> previousStatsMap = new HashMap<>();
        private final Thread timeTrackerThread;
        private final long millisToKeepHotKey;
        private final long accessesPerMillisecondForHot;
        private final AtomicLong hotKeyAccesses = new AtomicLong();
        private final AtomicLong nonHotKeyAccesses = new AtomicLong();

        ReducerMonitor(long accessesPerMillisecondForHot, long millisToKeepHotKey) {
            this.accessesPerMillisecondForHot = accessesPerMillisecondForHot;
            this.millisToKeepHotKey = millisToKeepHotKey;
            if (this.accessesPerMillisecondForHot > 1) {
                timeTrackerThread = new Thread(() -> {
                    while (true) {
                        try {
                            Thread.sleep(1000);
                            slipTime();
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }, "timeTrackerThread");
                timeTrackerThread.setDaemon(true);
                timeTrackerThread.start();
            }
            else {
                // Each access would tag a key as hot, so just disable this functionality.
                timeTrackerThread = null;
            }
        }

        private synchronized void slipTime() {
            previousStatsMap = currentStatsMap;
            currentStatsMap = new HashMap<>();
        }

        private synchronized Stats getStatsForKey(Key key) {
            Stats stats = currentStatsMap.get(key);
            if (stats != null) {
                return stats;
            }
            stats = previousStatsMap.get(key);
            if (stats != null) {
                currentStatsMap.put(key, stats);
                previousStatsMap.remove(key);
            }
            else {
                stats = new Stats();
                currentStatsMap.put(key, stats);
            }
            return stats;
        }

        boolean useReducer(Key key) {
            boolean result;
            if (timeTrackerThread == null) {
                result = true;
            }
            else {
                long now = System.currentTimeMillis();
                Stats stats = getStatsForKey(key);
                long expiry = stats.expiry.get();
                if (expiry < now) {
                    stats.expiry.set(now);
                    stats.counter.set(1);
                    result = false;
                }
                else if (expiry == now || (expiry + 1) == now) {
                    long count = stats.counter.incrementAndGet();
                    if (count >= accessesPerMillisecondForHot) {
                        stats.expiry.set(now + millisToKeepHotKey);
                        result = true;
                    }
                    else {
                        stats.expiry.set(now);
                        result = false;
                    }
                }
                else {
                    stats.expiry.set(now + millisToKeepHotKey);
                    result = true;
                }
            }
            if (result) {
                hotKeyAccesses.incrementAndGet();
            }
            else {
                nonHotKeyAccesses.incrementAndGet();
            }
            return result;
        }
    }

    /** Statistics about the reducer's hot-key detection effectiveness and delay timing. */
    public static class Statistics {
        private final Duration desiredDuration;
        private final long actualDurationNs;
        private final long hotKeyAccesses;
        private final long nonHotKeyAccesses;

        Statistics(long hotKeyAccesses, long nonHotKeyAccesses, Duration desiredDuration, long actualDurationNs) {
            this.hotKeyAccesses = hotKeyAccesses;
            this.nonHotKeyAccesses = nonHotKeyAccesses;
            this.desiredDuration = desiredDuration;
            this.actualDurationNs = actualDurationNs;
        }

        public long getHotKeyAccesses() {
            return hotKeyAccesses;
        }

        public long getNonHotKeyAccesses() {
            return nonHotKeyAccesses;
        }

        @Override
        public String toString() {
            return "Statistics [hotKeyAccesses=" + hotKeyAccesses + ", nonHotKeyAccesses=" + nonHotKeyAccesses + ","
                    + " desiredDelayTime=" + desiredDuration + ", actualDelayTime=" + (actualDurationNs / 1000) + "us]";
        }
    }

    private final Map<Key, OperationBatch> operationMap = new ConcurrentHashMap<>();
    private final Session session;
    private final long delayTimeMs;
    private final int delayTimeNs;
    private final ReducerMonitor monitor;
    private final Duration desiredDelayDuration;
    private final long actualDelayDurationNs;

    /**
     * Creates a HotKeyReducer with full configuration options.
     *
     * @param session the Session to use for the batched operate calls
     * @param delayTime the time to wait before executing batched operations (minimum 1 microsecond)
     * @param accessesPerMillisecondForHot minimum accesses per millisecond for a key to be considered hot
     * @param millisToKeepHotKey duration in milliseconds to keep a key hot once detected
     */
    public HotKeyReducer(Session session, Duration delayTime, long accessesPerMillisecondForHot, long millisToKeepHotKey) {
        long delayTimeNanos = delayTime.get(ChronoUnit.NANOS);
        this.delayTimeMs = delayTimeNanos / 1_000_000;
        this.delayTimeNs = (int) (delayTimeNanos % 1_000_000);

        if (delayTimeMs < 0 || (delayTimeMs == 0 && delayTimeNs < 1000)) {
            throw new IllegalArgumentException("Delay time must be at least 1us, not " + delayTimeMs);
        }
        this.session = session;
        this.monitor = new ReducerMonitor(accessesPerMillisecondForHot, millisToKeepHotKey);

        long now = System.nanoTime();
        try {
            Thread.sleep(this.delayTimeMs, this.delayTimeNs);
        }
        catch (InterruptedException e) {
        }
        this.actualDelayDurationNs = System.nanoTime() - now;
        this.desiredDelayDuration = delayTime;
    }

    private synchronized boolean addOpsForKey(Key key, CompletableFuture<Record> future, Operation... operations) {
        OperationBatch ops = operationMap.get(key);
        boolean shouldDelay = false;
        if (ops == null) {
            ops = new OperationBatch();
            operationMap.put(key, ops);
            shouldDelay = true;
        }
        ops.submitOperationCall(future, operations);
        return shouldDelay;
    }

    private synchronized OperationBatch getBatchAndRemoveFromMap(Key key) {
        return operationMap.remove(key);
    }

    /** Synchronous version of {@link #submitAsync}; blocks until the operation completes. */
    public Record submit(Key key, Operation... ops) {
        try {
            return submitAsync(key, ops).join();
        }
        catch (CompletionException ce) {
            Throwable cause = ce.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw ce;
        }
    }

    /**
     * Submits operations for execution, batching them with other operations for the same key if
     * it's determined to be hot.
     */
    public CompletableFuture<Record> submitAsync(Key key, Operation... ops) {
        CompletableFuture<Record> myResults = new CompletableFuture<>();
        OperationBatch operationBatch = null;
        try {
            if (!monitor.useReducer(key)) {
                myResults.complete(session.upsert(key).appendOperations(ops)
                        .execute().getFirst().orElseThrow().recordOrThrow());
            }
            else if (addOpsForKey(key, myResults, ops)) {
                Thread.sleep(delayTimeMs, delayTimeNs);
                operationBatch = getBatchAndRemoveFromMap(key);

                Record results = session.upsert(key).appendOperations(operationBatch.getOperations())
                        .execute().getFirst().orElseThrow().recordOrThrow();
                if (operationBatch.isSingleOperation()) {
                    myResults.complete(results);
                }
                else {
                    operationBatch.finish(results);
                }
            }
        }
        catch (Exception e) {
            if (operationBatch != null) {
                operationBatch.setFuturesWithException(e);
            }
            else {
                myResults.completeExceptionally(e);
            }
        }
        return myResults;
    }

    public Statistics getStatistics() {
        return new Statistics(monitor.hotKeyAccesses.get(), monitor.nonHotKeyAccesses.get(), desiredDelayDuration, actualDelayDurationNs);
    }
}
