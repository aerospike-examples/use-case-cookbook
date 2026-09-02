package com.aerospike.examples.hotkeys;

import java.time.Duration;

import com.aerospike.client.sdk.Bin;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Operation;
import com.aerospike.client.sdk.Session;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.hotkeys.helper.HotKeyReducer;
import com.aerospike.mapper.tools.AeroMapper;

/**
 * SDK port of the legacy {@code ReducerHotKeyUseCase} (see ../../java). Demonstrates programmatic
 * hot-key reduction: concurrent operate calls against one key are batched inside the JVM by
 * {@link HotKeyReducer} before being sent to Aerospike.
 * <p/>
 * <b>Baseline:</b> each thread calls {@code session.upsert(key)...execute()} directly on the hot key.
 * <p/>
 * <b>Mitigation:</b> the same operations are submitted through {@link HotKeyReducer}, which
 * coalesces operations for hot keys within a short delay window.
 * <p/>
 * <b>REVIEW:</b> Tuning {@link #REDUCER_DELAY_MS}, {@link #HOT_THRESHOLD}, and
 * {@link #HOT_DURATION_MS} strongly affects batching behaviour and latency. Adjust these for your
 * workload and cluster settings.
 */
public class ReducerHotKeyUseCase implements UseCase {

    // REVIEW: delay before flushing a batch - lower = less latency, less batching opportunity
    private static final int REDUCER_DELAY_MS = 1;
    // REVIEW: accesses in the same millisecond before a key is treated as hot
    private static final int HOT_THRESHOLD = 3;
    // REVIEW: how long a key stays hot once detected
    private static final int HOT_DURATION_MS = 500;

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
    public void setup(Session session, AeroMapper mapper) throws Exception {
        // Reducer operates on a single key; replicas are not used but primary is seeded cleanly.
        HotKeyProductSetup.truncateAndSeed(session, 1);
    }

    @Override
    public void run(Session session, AeroMapper mapper) throws Exception {
        long productId = HotKeySimulationParams.HOT_PRODUCT_ID;
        int numThreads = HotKeySimulationParams.NUM_THREADS;
        int durationSecs = HotKeySimulationParams.DURATION_SECS;

        Key primaryKey = HotKeyKeys.primary(productId);

        try (HotKeyPendingLimitScope ignored = HotKeyPendingLimitScope.apply(session, HotKeyKeys.PRODUCTS.getNamespace(),
                HotKeySimulationParams.TRANSACTION_PENDING_LIMIT)) {
            HotKeySimulationStats baseline = simulation.run("Baseline - direct operate on hot key",
                    numThreads, durationSecs,
                    () -> session.upsert(primaryKey).bin("unitsSold").add(1).execute().close());

            // REVIEW: HotKeyReducer is vendored from hot-key-reducer; verify delay/threshold for your cluster.
            HotKeyReducer reducer = new HotKeyReducer(session,
                    Duration.ofMillis(REDUCER_DELAY_MS), HOT_THRESHOLD, HOT_DURATION_MS);

            HotKeySimulationStats mitigated = simulation.run("Mitigation - HotKeyReducer batching",
                    numThreads, durationSecs,
                    () -> reducer.submit(primaryKey, Operation.add(new Bin("unitsSold", 1))));

            System.out.println("HotKeyReducer statistics: " + reducer.getStatistics());
            System.out.printf("Primary record unitsSold after mitigation: %d%n",
                    HotKeyProductSetup.readUnitsSold(session, primaryKey));
            System.out.printf("Mitigation successes: %,d (baseline successes: %,d)%n",
                    mitigated.successes(), baseline.successes());
        }
    }
}
