package com.aerospike.examples.hotkeys;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Session;
import com.aerospike.examples.UseCase;

/**
 * SDK port of the legacy {@code WriteHotKeyUseCase} (see ../../java). Demonstrates a write hot
 * key: many threads increment {@code unitsSold} on the same product.
 * <p/>
 * <b>Baseline:</b> all writes target the primary key {@code productId}.
 * <p/>
 * <b>Mitigation:</b> writes are directed to a random shard {@code productId:index}; a batch read
 * merges {@code unitsSold} across shards when the logical total is needed.
 * <p/>
 * Both phases also read the logical total about once every 5ms so occasional reads occur alongside
 * the write-heavy load.
 */
public class WriteHotKeyUseCase implements UseCase {

    private final HotKeySimulation simulation = new HotKeySimulation();

    @Override
    public String getName() {
        return "Hot Key - Write (Shard + Merge)";
    }

    @Override
    public String getDescription() {
        return "Simulate a write hot key by incrementing unitsSold on one record, then repeat with "
                + "sharded writes and batch-read merge to reconstruct the logical total.";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/hot-key-write-shard-merge.md";
    }

    @Override
    public void setup(Session session) throws Exception {
        HotKeyProductSetup.truncateAndSeed(session, HotKeySimulationParams.REPLICA_COUNT);
    }

    @Override
    public void run(Session session) throws Exception {
        long productId = HotKeySimulationParams.HOT_PRODUCT_ID;
        int numThreads = HotKeySimulationParams.NUM_THREADS;
        int durationSecs = HotKeySimulationParams.DURATION_SECS;
        int replicaCount = HotKeySimulationParams.REPLICA_COUNT;

        Key primaryKey = HotKeyKeys.primary(productId);
        long sideOpIntervalMs = HotKeySimulationParams.PERIODIC_SIDE_OP_INTERVAL_MS;

        try (HotKeyPendingLimitScope ignored = HotKeyPendingLimitScope.apply(session, HotKeyKeys.products().getNamespace(),
                HotKeySimulationParams.TRANSACTION_PENDING_LIMIT)) {
            HotKeySimulationStats baseline = simulation.run("Baseline - single write hot key", numThreads,
                    durationSecs, () -> session.upsert(primaryKey).bin("unitsSold").add(1).execute().close(),
                    () -> session.query(primaryKey).readingOnlyBins("unitsSold").execute().close(),
                    sideOpIntervalMs);

            System.out.printf("Primary record unitsSold after baseline: %d%n",
                    session.query(primaryKey).execute().getFirst().orElseThrow().recordOrThrow().getInt("unitsSold"));

            HotKeySimulationStats mitigated = simulation.run("Mitigation - random write shard", numThreads,
                    durationSecs, () -> {
                        Key shardKey = HotKeyKeys.randomReplica(productId, replicaCount);
                        session.upsert(shardKey).bin("unitsSold").add(1).execute().close();
                    },
                    // Batch read all shard keys and sum unitsSold (~every 5ms) - see readMergedUnitsSold
                    () -> HotKeyProductSetup.readMergedUnitsSold(session, replicaCount),
                    sideOpIntervalMs);

            // Same batch-read merge: session.query(keys).readingOnlyBins("unitsSold") then sum across shards
            int mergedTotal = HotKeyProductSetup.readMergedUnitsSold(session, replicaCount);
            System.out.printf("Merged unitsSold across %d shards after mitigation: %d%n", replicaCount, mergedTotal);
            System.out.printf("Mitigation successes: %,d (baseline successes: %,d)%n",
                    mitigated.successes(), baseline.successes());
        }
    }
}
