package com.aerospike.examples.hotkeys;

import java.time.Duration;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.examples.Parameter;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.hotkeys.helper.HotKeyReducer;
import com.aerospike.mapper.tools.AeroMapper;

/**
 * Demonstrates programmatic hot-key reduction: concurrent {@code operate} calls against one key are
 * batched inside the JVM by {@link HotKeyReducer} before being sent to Aerospike.
 *
 * <p><b>Baseline:</b> each thread calls {@code client.operate} directly on the hot key.
 *
 * <p><b>Mitigation:</b> the same operations are submitted through {@link HotKeyReducer}, which
 * coalesces operations for hot keys within a short delay window.
 *
 * <p><b>REVIEW:</b> Tuning {@link #REDUCER_DELAY_MS}, {@link #HOT_THRESHOLD}, and
 * {@link #HOT_DURATION_MS} strongly affects batching behaviour and latency. Adjust these for your
 * workload and cluster settings.
 */
public class ReducerHotKeyUseCase implements UseCase {

    // REVIEW: delay before flushing a batch — lower = less latency, less batching opportunity
    private static final Parameter<Integer> REDUCER_DELAY_MS = new Parameter<>(
            "reducerDelayMs",
            1,
            "HotKeyReducer batch delay in milliseconds");

    // REVIEW: accesses in the same millisecond before a key is treated as hot
    private static final Parameter<Integer> HOT_THRESHOLD = new Parameter<>(
            "hotThreshold",
            3,
            "Accesses per millisecond before HotKeyReducer treats a key as hot");

    // REVIEW: how long a key stays hot once detected
    private static final Parameter<Integer> HOT_DURATION_MS = new Parameter<>(
            "hotDurationMs",
            500,
            "Milliseconds to keep a key hot after detection");

    private IAerospikeClient client;
    private AeroMapper mapper;
    private final HotKeySimulation simulation = new HotKeySimulation();

    @Override
    public String getName() {
        return "Hot Key - Write (HotKeyReducer)";
    }

    @Override
    public String getDescription() {
        return "Simulate a write hot key with direct operate calls, then repeat using HotKeyReducer "
                + "to batch concurrent operations on the same key within the JVM.";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/hot-key-write-reducer.md";
    }

    @Override
    public String[] getTags() {
        return new String[] { "Hot Keys", "Performance", "Write Path", "Batching" };
    }

    @Override
    public Parameter<?>[] getParams() {
        return new Parameter<?>[] {
                HotKeySimulationParams.NUM_THREADS,
                HotKeySimulationParams.DURATION_SECS,
                HotKeySimulationParams.TRANSACTION_PENDING_LIMIT,
                REDUCER_DELAY_MS,
                HOT_THRESHOLD,
                HOT_DURATION_MS
        };
    }

    @Override
    public void setup(IAerospikeClient client, AeroMapper mapper) throws Exception {
        this.client = client;
        this.mapper = mapper;
        // Reducer operates on a single key; replicas are not used but primary is seeded cleanly.
        HotKeyProductSetup.truncateAndSeed(client, mapper, 1);
    }

    @Override
    public void run(IAerospikeClient client, AeroMapper mapper) throws Exception {
        this.client = client;
        this.mapper = mapper;

        long productId = HotKeySimulationParams.HOT_PRODUCT_ID;
        int numThreads = HotKeySimulationParams.NUM_THREADS.get();
        int durationSecs = HotKeySimulationParams.DURATION_SECS.get();

        Key primaryKey = HotKeyKeys.primary(mapper, productId);

        try (HotKeyPendingLimitScope ignored = HotKeyPendingLimitScope.apply(client, mapper,
                HotKeySimulationParams.TRANSACTION_PENDING_LIMIT.get())) {
            HotKeySimulationStats baseline = simulation.run("Baseline - direct operate on hot key",
                    numThreads, durationSecs,
                    () -> client.operate(null, primaryKey, Operation.add(new Bin("unitsSold", 1))));

            // REVIEW: HotKeyReducer is vendored from hot-key-reducer; verify delay/threshold for your cluster.
            HotKeyReducer reducer = new HotKeyReducer(
                    client,
                    Duration.ofMillis(REDUCER_DELAY_MS.get()),
                    HOT_THRESHOLD.get(),
                    HOT_DURATION_MS.get());

            HotKeySimulationStats mitigated = simulation.run("Mitigation - HotKeyReducer batching",
                    numThreads, durationSecs,
                    () -> reducer.submit(null, primaryKey, Operation.add(new Bin("unitsSold", 1))));

            System.out.println("HotKeyReducer statistics: " + reducer.getStatistics());
            System.out.printf("Primary record unitsSold after mitigation: %d%n",
                    client.get(null, primaryKey, "unitsSold").getInt("unitsSold"));
            System.out.printf("Mitigation successes: %,d (baseline successes: %,d)%n",
                    mitigated.successes(), baseline.successes());
        }
    }
}
