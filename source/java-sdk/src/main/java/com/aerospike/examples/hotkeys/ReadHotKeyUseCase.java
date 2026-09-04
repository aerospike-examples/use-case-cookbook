package com.aerospike.examples.hotkeys;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Session;
import com.aerospike.examples.UseCase;
import com.aerospike.mapper.tools.AeroMapper;

/**
 * SDK port of the legacy {@code ReadHotKeyUseCase} (see ../../java). Demonstrates a read hot key:
 * many threads repeatedly read the same product record.
 * <p/>
 * <b>Baseline:</b> all reads target the primary key {@code productId}.
 * <p/>
 * <b>Mitigation:</b> identical copies are stored at {@code productId:0..N-1}; each read picks a
 * random replica so load is spread across multiple records.
 * <p/>
 * Both phases also increment {@code unitsSold} on the primary key and every replica (~every 5ms)
 * via a batch write so all copies stay in sync alongside the read-heavy load.
 */
public class ReadHotKeyUseCase implements UseCase {

    private final HotKeySimulation simulation = new HotKeySimulation();

    @Override
    public String getName() {
        return "Hot Key - Read (Replica Spread)";
    }

    @Override
    public String getDescription() {
        return "Simulate a read hot key against a single product record, then repeat with identical "
                + "replica records and random replica selection to spread read load.";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/hot-key-read-replica-spread.md";
    }

    @Override
    public void setup(Session session, AeroMapper mapper) throws Exception {
        HotKeyProductSetup.truncateAndSeed(session, HotKeySimulationParams.REPLICA_COUNT);
    }

    @Override
    public void run(Session session, AeroMapper mapper) throws Exception {
        long productId = HotKeySimulationParams.HOT_PRODUCT_ID;
        int numThreads = HotKeySimulationParams.NUM_THREADS;
        int durationSecs = HotKeySimulationParams.DURATION_SECS;
        int replicaCount = HotKeySimulationParams.REPLICA_COUNT;

        Key primaryKey = HotKeyKeys.primary(productId);
        long sideOpIntervalMs = HotKeySimulationParams.PERIODIC_SIDE_OP_INTERVAL_MS;
        HotKeySimulation.OperationAttempt refreshAllCopies = () ->
                HotKeyProductSetup.incrementUnitsSoldOnAllCopies(session, replicaCount);

        try (HotKeyPendingLimitScope ignored = HotKeyPendingLimitScope.apply(session, HotKeyKeys.PRODUCTS.getNamespace(),
                HotKeySimulationParams.TRANSACTION_PENDING_LIMIT)) {
            simulation.run("Baseline - single read hot key", numThreads, durationSecs, () -> {
                session.query(primaryKey).readingOnlyBins("sku", "description", "unitsSold").execute().close();
            }, refreshAllCopies, sideOpIntervalMs);

            simulation.run("Mitigation - random read replica", numThreads, durationSecs, () -> {
                Key replicaKey = HotKeyKeys.randomReplica(productId, replicaCount);
                session.query(replicaKey).readingOnlyBins("sku", "description", "unitsSold").execute().close();
            }, refreshAllCopies, sideOpIntervalMs);
        }
    }
}
