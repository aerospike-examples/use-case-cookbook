package com.aerospike.examples.hotkeys;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.examples.Parameter;
import com.aerospike.examples.UseCase;
import com.aerospike.mapper.tools.AeroMapper;

/**
 * Demonstrates a write hot key: many threads increment {@code unitsSold} on the same product.
 *
 * <p><b>Baseline:</b> all writes target the primary key {@code productId}.
 *
 * <p><b>Mitigation:</b> writes are directed to a random shard {@code productId:index}; a batch read
 * merges {@code unitsSold} across shards when the logical total is needed.
 *
 * <p>Both phases also read the logical total about once every 100ms so occasional reads occur
 * alongside the write-heavy load.
 */
public class WriteHotKeyUseCase implements UseCase {

    private IAerospikeClient client;
    private AeroMapper mapper;
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
    public String[] getTags() {
        return new String[] { "Hot Keys", "Performance", "Write Path", "Batch Reads" };
    }

    @Override
    public Parameter<?>[] getParams() {
        return new Parameter<?>[] {
                HotKeySimulationParams.NUM_THREADS,
                HotKeySimulationParams.DURATION_SECS,
                HotKeySimulationParams.REPLICA_COUNT,
                HotKeySimulationParams.TRANSACTION_PENDING_LIMIT
        };
    }

    @Override
    public void setup(IAerospikeClient client, AeroMapper mapper) throws Exception {
        this.client = client;
        this.mapper = mapper;
        HotKeyProductSetup.truncateAndSeed(client, mapper, HotKeySimulationParams.REPLICA_COUNT.get());
    }

    @Override
    public void run(IAerospikeClient client, AeroMapper mapper) throws Exception {
        this.client = client;
        this.mapper = mapper;

        long productId = HotKeySimulationParams.HOT_PRODUCT_ID;
        int numThreads = HotKeySimulationParams.NUM_THREADS.get();
        int durationSecs = HotKeySimulationParams.DURATION_SECS.get();
        int replicaCount = HotKeySimulationParams.REPLICA_COUNT.get();

        Key primaryKey = HotKeyKeys.primary(mapper, productId);
        long sideOpIntervalMs = HotKeySimulationParams.PERIODIC_SIDE_OP_INTERVAL_MS;

        try (HotKeyPendingLimitScope ignored = HotKeyPendingLimitScope.apply(client, mapper,
                HotKeySimulationParams.TRANSACTION_PENDING_LIMIT.get())) {
            HotKeySimulationStats baseline = simulation.run("Baseline - single write hot key", numThreads,
                    durationSecs, () -> client.operate(null, primaryKey, Operation.add(new Bin("unitsSold", 1))),
                    () -> client.get(null, primaryKey, "unitsSold"), sideOpIntervalMs);

            System.out.printf("Primary record unitsSold after baseline: %d%n",
                    client.get(null, primaryKey, "unitsSold").getInt("unitsSold"));

            HotKeySimulationStats mitigated = simulation.run("Mitigation - random write shard", numThreads,
                    durationSecs, () -> {
                        Key shardKey = HotKeyKeys.randomReplica(mapper, productId, replicaCount);
                        client.operate(null, shardKey, Operation.add(new Bin("unitsSold", 1)));
                    },
                    // Batch read all shard keys and sum unitsSold (~every 100ms) — see readMergedUnitsSold
                    () -> HotKeyProductSetup.readMergedUnitsSold(client, mapper, replicaCount),
                    sideOpIntervalMs);

            // Same batch-read merge: client.get(keys[], "unitsSold") then sum across shards
            int mergedTotal = HotKeyProductSetup.readMergedUnitsSold(client, mapper, replicaCount);
            System.out.printf("Merged unitsSold across %d shards after mitigation: %d%n", replicaCount,
                    mergedTotal);
            System.out.printf("Mitigation successes: %,d (baseline successes: %,d)%n",
                    mitigated.successes(), baseline.successes());
        }
    }
}
