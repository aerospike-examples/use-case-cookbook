package com.aerospike.examples.hotkeys.helper;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Txn;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.WritePolicy;

/**
 * Vendored from the standalone hot-key-reducer project for use-case-cookbook demonstrations.
 * Original package: {@code com.aerospike.helper}. See {@code ReducerHotKeyUseCase} for integration
 * and tuning notes marked REVIEW. In-memory batches are partitioned by {@code (Key, Txn)} so
 * transactional and non-transactional operations are never coalesced into one server operate.
 *
 * A utility class for reducing hot key contention in Aerospike operations by intelligently
 * batching operations for frequently accessed keys.
 * 
 * <p>The HotKeyReducer monitors key access patterns to Aerospike and automatically batches operations
 * for keys that are accessed frequently within short time windows into a single {@operate} command. 
 * This helps reduce contention and improves performance when multiple clients are accessing the same keys
 * simultaneously.</p>
 * 
 * <p>Key features:</p>
 * <ul>
 *   <li>Automatic hot key detection based on configurable thresholds</li>
 *   <li>Operation batching with configurable delay times</li>
 *   <li>Asynchronous and synchronous operation support</li>
 *   <li>Statistics tracking for monitoring reducer effectiveness</li>
 * </ul>
 * 
 * <p>Example usage:</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * HotKeyReducer reducer = new HotKeyReducer(client, Duration.ofMillis(5));
 * 
 * // Synchronous operation
 * Record result = reducer.submit(null, key, Operation.get());
 * 
 * // Asynchronous operation
 * CompletableFuture<Record> future = reducer.submitAsync(null, key, Operation.get());
 * }</pre>
 * 
 * @author Aerospike
 * @since 1.0
 */
public class HotKeyReducer {
    /**
     * A simple map of integers which can be incremented or retrieved. 
     * This class is not thread safe and is used internally for tracking operation counts.
     * 
     * <p>This utility class provides a convenient way to maintain counters for string keys,
     * automatically initializing counters to 1 on first access and incrementing thereafter.</p>
     */
    private static class CounterMap {
        private final Map<String, Integer> map = new HashMap<>();
        /**
         * Increments the counter for the given name and returns the new value.
         * If the counter doesn't exist, it is initialized to 1.
         * 
         * @param name the name of the counter to increment
         * @return the new counter value after incrementing
         */
        public int increment(String name) {
            Integer value = map.get(name);
            if (value == null) {
                map.put(name, 1);
                return 1;
            }
            else {
                map.put(name, value+1);
                return value+1;
            }
        }
        /**
         * Retrieves the current value of the counter for the given name.
         * 
         * @param name the name of the counter to retrieve
         * @return the current counter value, or 0 if the counter doesn't exist
         */
        public int get(String name) {
            Integer value = map.get(name);
            return value == null ? 0 : value;
        }
        
        @Override
        public String toString() {
            return this.map.toString();
        }
    }

    /**
     * Identifies an in-memory batch by Aerospike key and transaction context. Operations with
     * different {@code Txn} references (or {@code null} vs non-null) never share a batch.
     */
    private static final class BatchKey {
        private final Key key;
        private final Txn txn;

        private BatchKey(Key key, Txn txn) {
            this.key = key;
            this.txn = txn;
        }

        static BatchKey of(Key key, WritePolicy writePolicy) {
            return new BatchKey(key, writePolicy.txn);
        }

        Key getKey() {
            return key;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof BatchKey)) {
                return false;
            }
            BatchKey other = (BatchKey) obj;
            return key.equals(other.key) && txn == other.txn;
        }

        @Override
        public int hashCode() {
            int result = key.hashCode();
            result = 31 * result + System.identityHashCode(txn);
            return result;
        }
    }

    /**
     * Represents the details for a single operation within an Aerospike operate call.
     * 
     * <p>This class encapsulates an individual operation along with metadata about
     * its position within the batch of operations. The binIndex is used to track
     * the order of operations on the same bin name across multiple operate calls.</p>
     */
    private static class SubmittedOp {
        private final Operation op;
        private final int binIndex;
        /**
         * Creates a new SubmittedOp with the given operation and counter tracking.
         * 
         * @param op the Aerospike operation to track
         * @param allOperateCalls counter map tracking operations across all operate calls
         * @param thisOperateCall counter map tracking operations for the current operate call
         */
        public SubmittedOp(Operation op, CounterMap allOperateCalls, CounterMap thisOperateCall) {
            this.op = op;
            int count = allOperateCalls.increment(this.op.binName);
            thisOperateCall.increment(this.op.binName);
            this.binIndex = count-1;
        }
        
        /**
         * Returns the underlying Aerospike operation.
         * 
         * @return the operation associated with this submitted operation
         */
        public Operation getOperation() {
            return this.op;
        }
        
        /**
         * Returns the bin name for this operation.
         * 
         * @return the bin name from the underlying operation
         */
        public String getBinName() {
            return this.op.binName;
        }
        
        /**
         * Returns the index position of this operation for its bin name.
         * 
         * @return the zero-based index of this operation within all operations on the same bin
         */
        public int getBinIndex() {
            return binIndex;
        }
        @Override
        public String toString() {
            return String.format("SubmittedOp(%d, %s, %s)", getBinIndex(), getBinName(), getOperation().type); 
        }
    }
    
    /**
     * Represents all operations for a single "operate" call submitted to the reducer.
     * 
     * <p>This class manages a collection of operations that were submitted together
     * in a single {@code operate} call, along with the CompletableFuture that will receive
     * the result. It handles the complex task of unpacking aggregated results back
     * into the format expected by the original caller.</p>
     */
    private static class SubmittedOps {
        private final CompletableFuture<Record> future;
        private final List<SubmittedOp> operations = new ArrayList<>();
        private final CounterMap binCountsForThisOperate = new CounterMap();
        private final CounterMap binCountsForAllOperateCalls;
        
        /**
         * Creates a new SubmittedOps instance for the given operations and future.
         * 
         * @param future the CompletableFuture that will receive the result
         * @param allOperateCalls counter map tracking all operations across calls
         * @param ops the array of operations for this operate call
         */
        public SubmittedOps(CompletableFuture<Record> future, CounterMap allOperateCalls, Operation ... ops) {
            this.future = future;
            this.binCountsForAllOperateCalls = allOperateCalls;
            for (Operation thisOp : ops) {
                operations.add(new SubmittedOp(thisOp, allOperateCalls, binCountsForThisOperate));
            }
            
        }
        
        /**
         * Unpacks a combined record from multiple operate calls into the expected record
         * containing only the bins and values relevant to this specific operate call.
         * 
         * <p>When multiple operate calls are batched together, the result contains all
         * operations from all calls. This method extracts only the relevant results
         * for this particular operate call, handling cases where:
         * <ul>
         *   <li>A bin appears only once across all calls</li>
         *   <li>A bin appears multiple times across calls but once in this call</li>
         *   <li>A bin appears multiple times in this single call</li>
         * </ul>
         * 
         * @param aggregatedRecord the combined record containing results from all batched operations
         * @return a new record containing only the results relevant to this operate call
         */
        @SuppressWarnings("unchecked")
        private Record formRecordForTheseOps(Record aggregatedRecord) {
            Map<String, Object> bins = new HashMap<>();
            for (SubmittedOp thisOp : this.operations) {
                String binName = thisOp.getBinName();
                int binIndex = thisOp.getBinIndex();
                Object value = aggregatedRecord.getValue(binName);
                int totalCountForBin = binCountsForAllOperateCalls.get(binName);
                int thisCountForBin = binCountsForThisOperate.get(binName);
                if (totalCountForBin == 1) {
                    // This is the only value, just store it
                    bins.put(binName, value);
                }
                else {
                    // This must be a list of values
                    List<Object> valueList = (List<Object>)value;
                    if (valueList.get(binIndex) != null) {
                        if (thisCountForBin == 1) {
                            // Just need to map the value to a single element
                            bins.put(binName, valueList.get(binIndex));
                        }
                        else {
                            // There are multiple instances of this bin in this one operate command, a list is needed
                            if (!bins.containsKey(binName)) {
                                bins.put(binName, new ArrayList<>());
                            }
                            ((List<Object>)bins.get(binName)).add(valueList.get(binIndex));
                        }
                    }
                }
            }
            // The resulting list can contain nulls, which should be stripped out
            // For example, Operation.put("name", bob
//            if (thisCountForBin > 1) && ((List<Object>)bins.get(binName)))
            if (bins.size() == 0) {
                return null;
            }
            return new Record(bins, aggregatedRecord.generation, aggregatedRecord.expiration);
        }
        
        /**
         * Completes the future with the appropriate record extracted from the aggregated result.
         * 
         * @param aggregatedRecord the combined record from the batched operation
         */
        public void finish(Record aggregatedRecord) {
            if (aggregatedRecord == null) {
                future.complete(null);
            }
            else {
                Record rec = this.formRecordForTheseOps(aggregatedRecord);
                future.complete(rec);
            }
        }
        
        /**
         * Completes the future exceptionally with the given exception.
         * 
         * @param e the exception that caused the operation to fail
         */
        public void fail(Exception e) {
            future.completeExceptionally(e);
        }
    }
    
    /**
     * Contains all operations for a single key and transaction within a batching time window.
     *
     * <p>An OperationBatch aggregates multiple operate calls for the same {@link BatchKey} that occur
     * within the configured delay time window. For example, if 5 calls to HotKeyReducer.submit
     * occur for the same key, each containing 2 operations, the structure will be:</p>
     * <ul>
     *   <li>One OperationBatch for the key, containing:</li>
     *   <li>Five SubmittedOps instances (one per submit call), each containing:</li>
     *   <li>Two SubmittedOp instances (one per operation in the call)</li>
     * </ul>
     * 
     * <p>This hierarchical structure enables proper result mapping, allowing the aggregated
     * record from the single batched operation to be correctly distributed back to the
     * individual CompletableFutures of the original callers.</p>
     */
    private static class OperationBatch {
        private final CounterMap allOperationsCounter = new CounterMap();
        private final List<SubmittedOps> operationCalls = new ArrayList<>();
        private int operationCount = 0;
        
        private final WritePolicy writePolicy;
        
        public OperationBatch(WritePolicy writePolicy) {
            this.writePolicy = writePolicy;
        }
        
        public WritePolicy getWritePolicy() {
            return writePolicy;
        }
        /**
         * Adds a new operate call to this batch.
         * 
         * @param future the CompletableFuture that will receive the result for this operate call
         * @param operations the array of operations in this operate call
         */
        public void submitOperationCall(CompletableFuture<Record> future, Operation ...operations) {
            this.operationCalls.add(new SubmittedOps(future, allOperationsCounter, operations));
            operationCount += operations.length;
        }
        
        /**
         * Completes all futures in this batch exceptionally with the given exception.
         * 
         * @param e the exception to propagate to all waiting callers
         */
        public void setFuturesWithException(Exception e) {
            operationCalls.forEach(op -> op.fail(e));
        }
        
        /**
         * Returns a flattened array of all operations in this batch.
         * This array will be used for the single batched operate call to Aerospike.
         * 
         * @return an array containing all operations from all operate calls in this batch
         */
        public Operation[] getOperations() {
            // Benchmarks show that this is substantially faster than using stream based approach.
            Operation[] results = new Operation[operationCount];
            int index = 0;
            for (SubmittedOps theseOps : operationCalls) {
                for (SubmittedOp thisOpList : theseOps.operations) {
                    results[index++] = thisOpList.op;
                }
            }
            return results;
        }
        
        /**
         * Checks if this batch contains only a single operate call.
         * 
         * @return true if there is only one operate call in this batch, false otherwise
         */
        public boolean isSingleOperation() {
            return this.operationCalls.size() == 1;
        }

        
        /**
         * Distributes the aggregated results to all waiting operate calls in this batch.
         * 
         * @param results the record returned from the batched Aerospike operate call
         */
        public void finish(Record results) {
            this.operationCalls.forEach(ops -> ops.finish(results));
        }
    }
    
    /**
     * Internal statistics tracking for individual keys.
     * Contains expiry time and access counter for hot key detection.
     */
    private static class Stats {
        private AtomicLong expiry = new AtomicLong(0);
        private AtomicLong counter = new AtomicLong(0);
    }
    
    /**
     * Monitors key access patterns to determine when the reducer should be used.
     * 
     * <p>The ReducerMonitor tracks access frequency for each key and determines
     * whether a key should be considered "hot" based on configurable thresholds.
     * Hot keys have their operations batched together to reduce contention.</p>
     * 
     * <p>Key detection algorithm:</p>
     * <ul>
     *   <li>Keys that receive more than the threshold accesses per millisecond become hot</li>
     *   <li>Hot keys remain hot for a configurable duration</li>
     *   <li>A background thread periodically cleans up old statistics</li>
     * </ul>
     */
    private class ReducerMonitor {
        private Map<Key, Stats> currentStatsMap = new HashMap<>();
        private Map<Key, Stats> previousStatsMap = new HashMap<>();
        private final Thread timeTrackerThread;
        private final long millisToKeepHotKey;
        private final long accessesPerMillisecondForHot;
        private final AtomicLong hotKeyAccesses = new AtomicLong();
        private final AtomicLong nonHotKeyAccesses = new AtomicLong();
        
        /**
         * Creates a new ReducerMonitor with the specified hot key detection parameters.
         * 
         * @param accessesPerMillisecondForHot minimum accesses per millisecond for a key to be considered hot
         * @param millisToKeepHotKey duration in milliseconds to keep a key hot once detected
         */
        public ReducerMonitor(long accessesPerMillisecondForHot, long millisToKeepHotKey) {
            this.accessesPerMillisecondForHot = accessesPerMillisecondForHot;
            this.millisToKeepHotKey = millisToKeepHotKey;
            if (this.accessesPerMillisecondForHot > 1) {
                //
                timeTrackerThread = new Thread(
                        () -> {
                            while (true) {
                                try {
                                    Thread.sleep(1000);
                                    slipTime();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        },
                        "timeTrackerThread");
                timeTrackerThread.setDaemon(true);
                timeTrackerThread.start();
            }
            else {
                // Each access would tag a key as hot, so just disable this functionality.
                timeTrackerThread = null;
            }
        }
        
        /**
         * Moves current statistics to previous and creates new current map.
         * Called periodically to age out old statistics.
         */
        private synchronized void slipTime() {
            previousStatsMap = currentStatsMap;
            currentStatsMap = new HashMap<>();
        }

        /**
         * Retrieves or creates statistics for the given key.
         * 
         * @param key the Aerospike key to get statistics for
         * @return the Stats object for the key
         */
        private synchronized Stats getStatsForKey(Key key) {
            Stats stats = currentStatsMap.get(key);
            if (stats != null) {
                return stats;
            }
            stats = previousStatsMap.get(key);
            if (stats != null) {
                // Move prev to current
                currentStatsMap.put(key, stats);
                previousStatsMap.remove(key);
            }
            else {
                // Allocate new stats and put into current map
                stats = new Stats();
                currentStatsMap.put(key, stats);
            }
            return stats;
        }
        
        /**
         * Determines whether the reducer should be used for the given key.
         * 
         * <p>This method implements the hot key detection algorithm. It tracks
         * access patterns and returns true if operations for this key should
         * be batched (because the key is hot), or false if the operation
         * should be executed immediately.</p>
         * 
         * @param key the Aerospike key being accessed
         * @return true if the reducer should batch operations for this key, false otherwise
         */
        public boolean useReducer(Key key) {
            boolean result;
            if (timeTrackerThread == null) {
                result = true;
            }
            else {
                long now = System.currentTimeMillis();
                Stats stats = getStatsForKey(key);
                long expiry = stats.expiry.get();
                if (expiry < now) {
                    // This key is not hot
                    stats.expiry.set(now);
                    stats.counter.set(1);
                    result = false;
                }
                else if (expiry == now || (expiry+1) == now) {
                    // This is in the same millisecond
                    long count = stats.counter.incrementAndGet();
                    if (count >= accessesPerMillisecondForHot) {
                        // This is hot!
                        stats.expiry.set(now + millisToKeepHotKey);
                        result = true;
                    }
                    else {
                        stats.expiry.set(now);
                        result = false;
                    }
                }
                else {
                    // This is hot, keep it hot
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
    
    /**
     * Statistics about the reducer's operation, including hot key detection effectiveness
     * and timing information.
     * 
     * <p>This class provides insight into how well the reducer is working by tracking:
     * <ul>
     *   <li>Number of accesses that used the reducer (hot keys)</li>
     *   <li>Number of accesses that bypassed the reducer (non-hot keys)</li>
     *   <li>Configured vs actual delay timing</li>
     * </ul>
     */
    public static class Statistics {
        private final Duration desiredDuration;
        private final long actualDurationNs;
        private final long hotKeyAccesses;
        private final long nonHotKeyAccesses;
        
        /**
         * Creates a new Statistics instance.
         * 
         * @param hotKeyAccesses number of accesses that used the reducer
         * @param nonHotKeyAccesses number of accesses that bypassed the reducer
         * @param desiredDuration the configured delay duration
         * @param acutalDurationNs the actual measured delay duration in nanoseconds
         */
        public Statistics(long hotKeyAccesses, long nonHotKeyAccesses, Duration desiredDuration, long acutalDurationNs) {
            super();
            this.hotKeyAccesses = hotKeyAccesses;
            this.nonHotKeyAccesses = nonHotKeyAccesses;
            this.desiredDuration = desiredDuration;
            this.actualDurationNs = acutalDurationNs;
        }
        /**
         * Returns the number of key accesses that used the reducer.
         * 
         * @return the count of hot key accesses
         */
        public long getHotKeyAccesses() {
            return hotKeyAccesses;
        }
        /**
         * Returns the number of key accesses that bypassed the reducer.
         * 
         * @return the count of non-hot key accesses
         */
        public long getNonHotKeyAccesses() {
            return nonHotKeyAccesses;
        }
        @Override
        public String toString() {
            return "Statistics [hotKeyAccesses=" + hotKeyAccesses + ", nonHotKeyAccesses=" + nonHotKeyAccesses + ","
                    + " desiredDelayTime=" + desiredDuration + ", actualDelayTime=" + (actualDurationNs/1000) + "us]";
        }
        
    }
    
    // --------------------------------
    //
    // Main class
    //
    // --------------------------------
    private final Map<BatchKey, OperationBatch> operationMap = new ConcurrentHashMap<>();
    private final IAerospikeClient client;
    private final long delayTimeMs;
    private final int delayTimeNs;
    private final ReducerMonitor monitor;
    private final Duration desiredDelayDuration;
    private final long actualDelayDurationNs;
    
    /**
     * Creates a HotKeyReducer with default settings (1ms delay).
     * 
     * @param client the Aerospike client to use for operations
     */
    public HotKeyReducer(IAerospikeClient client) {
        this(client, Duration.ofMillis(1));
    }
    /**
     * Creates a HotKeyReducer with the specified delay time and default hot key detection settings.
     * 
     * @param client the Aerospike client to use for operations
     * @param delayTime the time to wait before executing batched operations
     */
    public HotKeyReducer(IAerospikeClient client, Duration delayTime) {
        this(client, delayTime, 3, 500);
    }
    
    /**
     * Creates a HotKeyReducer with full configuration options.
     * 
     * @param client the Aerospike client to use for operations
     * @param delayTime the time to wait before executing batched operations (minimum 1 microsecond)
     * @param accessesPerMillisecondForHot minimum accesses per millisecond for a key to be considered hot
     * @param millisToKeepHotKey duration in milliseconds to keep a key hot once detected
     * @throws IllegalArgumentException if delayTime is less than 1 microsecond
     */
    public HotKeyReducer(IAerospikeClient client, Duration delayTime,
            long accessesPerMillisecondForHot, long millisToKeepHotKey) {
        
        long delayTimeNs = delayTime.get(ChronoUnit.NANOS);
        this.delayTimeMs = delayTimeNs / 1_000_000;
        this.delayTimeNs = (int)(delayTimeNs % 1_000_000);
        
        if (delayTimeMs < 0 || (delayTimeMs == 0 && delayTimeNs < 1000)) {
            throw new IllegalArgumentException("Delay time must be at least 1us, not " +delayTimeMs);
        }
        this.client = client;
        this.monitor = new ReducerMonitor(accessesPerMillisecondForHot, millisToKeepHotKey);
        
        long now = System.nanoTime();
        try {
            Thread.sleep(this.delayTimeMs, this.delayTimeNs);
        } catch (InterruptedException e) {
        }
        this.actualDelayDurationNs = System.nanoTime() - now;
        this.desiredDelayDuration = delayTime;
    }
    
    /**
     * Adds operations to the batching system for a {@link BatchKey}.
     *
     * @param key the Aerospike batch key
     * @param future the future to complete with results
     * @param operations the operations to batch
     * @return true if this is the first operation for the batch key (indicating delay should occur)
     * @throws InterruptedException if the thread is interrupted
     */
    private synchronized boolean addOpsForBatch(WritePolicy writePolicy, BatchKey batchKey,
            CompletableFuture<Record> future, Operation ... operations) throws InterruptedException {
        OperationBatch ops = operationMap.get(batchKey);
        boolean shouldDelay = false;
        if (ops == null) {
            ops = new OperationBatch(writePolicy);
            operationMap.put(batchKey, ops);
            shouldDelay = true;
        }
        else {
            Expression filterToUse = ops.getWritePolicy() == null ? null : ops.getWritePolicy().filterExp;
            Expression thisFilter = writePolicy == null ? null : writePolicy.filterExp;
            if ((filterToUse == null && thisFilter != null) ||
                    (filterToUse != null && !filterToUse.equals(thisFilter))) {
                throw new AerospikeException("Filter expression on passed operation differs to filter expression on original operation");
            }
        }
        ops.submitOperationCall(future, operations);
        return shouldDelay;
    }

    /**
     * Retrieves and removes the operation batch for a key.
     * 
     * @param key the Aerospike key
     * @return the OperationBatch for the key, or null if not found
     */    private synchronized OperationBatch getBatchAndRemoveFromMap(BatchKey batchKey) {
        return operationMap.remove(batchKey);
    }

    /**
     * Submits operations for execution, potentially batching them with other operations
     * for the same key if it's determined to be hot.
     * 
     * <p>This is the synchronous version of the submit method. It blocks until the
     * operation completes and returns the result directly.</p>
     * 
     * <p><b>Note:</b> Hot-key detection is per {@code Key}, but in-memory batches are partitioned by
     * {@code (Key, Txn)}. Operations with different transaction contexts (including {@code null}
     * vs non-null {@code wp.txn}) coalesce independently. Within a batch, the first submitter's
     * {@link WritePolicy} is used for the server operate (timeouts, etc.). All operations in a
     * batch must share the same {@code filterExp}; mismatches throw {@link AerospikeException}.</p>
     * <p>Filter expressions can be converted into read or write operations with a condition.
     * For example:</p>
     * <pre>
     * WritePolicy wp = client.copyWritePolicyDefault();
     * wp.filterExp = Exp.build(Exp.eq(Exp.stringBin("name"), Exp.val("Bob")));
     * Record rec = reducer.submit(wp, key, Operation.put(new Bin("name", "Tim")));
     * </pre>
     *
     * <p>This says "if the 'name' bin is 'Bob', set the 'name' bin to 'Tim'. But if there is a
     * concurrent operation on the same key with either no filter or a different filter, an
     * exception will be thrown.</p>
     *
     * <p>The solution is to turn this into a write expression which does the same thing:</p>
     * <pre>
     * WritePolicy wp = client.copyWritePolicyDefault();
     * wp.filterExp = null;
     * Expression exp = Exp.build(Exp.cond(Exp.eq(Exp.stringBin("name"), Exp.val("Bob")),
     *                      Exp.val("Tim"), Exp.unknown()));
     * Record rec = reducer.submit(wp, key, ExpOperation.write("name", exp, ExpWriteFlags.EVAL_NO_FAIL));
     * </pre>
     *
     * @param wp the write policy to use (null for default policy)
     * @param key the Aerospike key to operate on
     * @param ops the operations to perform
     * @return the Record result from the operations
     * @throws RuntimeException if the operation fails
     */
    public Record submit(WritePolicy wp, Key key, Operation ... ops) {
        try {
            return submitAsync(wp, key, ops).join();
        }
        catch (CompletionException ce) {
            Throwable cause = ce.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException)cause;
            }
            else {
                throw ce;
            }
        }
    }
    
    /**
     * Asynchronously submits operations for execution, potentially batching them with 
     * other operations for the same key if it's determined to be hot.
     * 
     * <p>The reducer determines whether to batch operations based on the key's access
     * pattern. For hot keys, operations are delayed and batched together. For non-hot
     * keys, operations execute immediately.</p>
     * 
     * <p>When batching occurs:</p>
     * <ul>
     *   <li>The first operation for a hot {@code (Key, Txn)} triggers a delay</li>
     *   <li>Additional operations for the same batch key are added to the batch</li>
     *   <li>After the delay, all operations execute as a single Aerospike operate call</li>
     *   <li>Results are distributed back to individual CompletableFutures</li>
     * </ul>
     * <p><b>Note:</b> If there are hot keys being reduced then the write policy to be used
     * will be the used for all batched operations. The ramification of this is:</p>
     * <ul>
     * <li>Different timeout settings will fall back to that write policy<li>
     * <li>Filter expressions should be the same for all operations. </li>
     * <ul>
     * If different filter expressions are used for records in the same operation, an exception
     * is thrown as this cannot be supported. However, filter expressions can be converted
     * into read or write operaions with a condition. 
     * <p/>
     * For example:
     * <pre>
     * WritePolicy wp = client.copyWritePolicyDefault();
     * wp.filterExp = Exp.build(Exp.eq(Exp.stringBin("name"), Exp.val("Bob")));
     * Record rec = reducer.submit(wp, key, Operation.put(new Bin("name", "Tim")));
     * </pre>
     * 
     * This basically says "if the 'name' bin is 'Bob', set the 'name' bin to 'Tim'. But
     * if there is a concurrent operation on the same key with either no filter or a
     * different filter, an exception will be thrown. 
     * 
     * The solution is to turn this into a write expression which does the same thing.
     * In this case it would be:
     * <pre>
     * WritePolicy wp = client.copyWritePolicyDefault();
     * wp.filterExp = null;
     * Expression exp = Exp.build(Exp.cond(Exp.eq(Exp.stringBin("name"), Exp.val("Bob")),
     *                      Exp.val("Tim"), Exp.unknown()));
     * Record rec = reducer.submit(wp, key, ExpOperation.write("name", exp, ExpWriteFlags.EVAL_NO_FAIL));
     * </pre>

     *
     * <p><b>Note:</b> Hot-key detection is per {@code Key}, but in-memory batches are partitioned by
     * {@code (Key, Txn)}. Operations with different transaction contexts (including {@code null}
     * vs non-null {@code wp.txn}) coalesce independently. Within a batch, the first submitter's
     * {@link WritePolicy} is used for the server operate. All operations in a batch must share
     * the same {@code filterExp}; mismatches throw {@link AerospikeException}.</p>
     *
     * @param wp the write policy to use (null for default policy)
     * @param key the Aerospike key to operate on
     * @param ops the operations to perform
     * @return a CompletableFuture that will be completed with the Record result
     */
    public CompletableFuture<Record> submitAsync(WritePolicy wp, Key key, Operation ... ops) {
        CompletableFuture<Record> myResults = new CompletableFuture<>();
        OperationBatch operationBatch = null;
        try {
            if (wp == null) {
                wp = client.copyWritePolicyDefault();
            }
            BatchKey batchKey = BatchKey.of(key, wp);
            if (!monitor.useReducer(key)) {
                myResults.complete(client.operate(wp, key, ops));
            }
            else if (addOpsForBatch(wp, batchKey, myResults, ops)) {
                Thread.sleep(delayTimeMs, delayTimeNs);
                operationBatch = getBatchAndRemoveFromMap(batchKey);
                WritePolicy batchWritePolicy = operationBatch.getWritePolicy();
                batchWritePolicy.respondAllOps = true;

                Record results = client.operate(batchWritePolicy, batchKey.getKey(),
                        operationBatch.getOperations());
                // If there is only one set of operations, just return it.
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
    
    /**
     * Returns statistics about the reducer's operation.
     * 
     * <p>The statistics include information about hot key detection effectiveness
     * and timing accuracy, which can be used to tune the reducer's configuration.</p>
     * 
     * @return a Statistics object containing operational metrics
     */
    public Statistics getStatistics() {
        return new Statistics(monitor.hotKeyAccesses.get(), monitor.nonHotKeyAccesses.get(), desiredDelayDuration, actualDelayDurationNs);
    }
}
