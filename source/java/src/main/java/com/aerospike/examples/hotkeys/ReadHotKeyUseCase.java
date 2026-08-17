package com.aerospike.examples.hotkeys;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.examples.Parameter;
import com.aerospike.examples.UseCase;
import com.aerospike.mapper.tools.AeroMapper;

/**
 * Demonstrates a read hot key: many threads repeatedly read the same product record.
 *
 * <p><b>Baseline:</b> all reads target the primary key {@code productId}.
 *
 * <p><b>Mitigation:</b> identical copies are stored at {@code productId:0..N-1}; each read picks
 * a random replica so load is spread across multiple records.
 *
 * <p>Both phases also increment {@code unitsSold} on the primary key and every replica (~every 100ms)
 * via a batch write so all copies stay in sync alongside the read-heavy load.
 */
public class ReadHotKeyUseCase implements UseCase {

    private IAerospikeClient client;
    private AeroMapper mapper;
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
    public String[] getTags() {
        return new String[] { "Hot Keys", "Performance", "Read Path" };
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
        HotKeySimulation.OperationAttempt refreshAllCopies = () -> HotKeyProductSetup.incrementUnitsSoldOnAllCopies(
                client, mapper, replicaCount);

        try (HotKeyPendingLimitScope ignored = HotKeyPendingLimitScope.apply(client, mapper,
                HotKeySimulationParams.TRANSACTION_PENDING_LIMIT.get())) {
            simulation.run("Baseline - single read hot key", numThreads, durationSecs, () -> {
                client.get(null, primaryKey, "sku", "description", "unitsSold");
            }, refreshAllCopies, sideOpIntervalMs);

            simulation.run("Mitigation - random read replica", numThreads, durationSecs, () -> {
                Key replicaKey = HotKeyKeys.randomReplica(mapper, productId, replicaCount);
                client.get(null, replicaKey, "sku", "description", "unitsSold");
            }, refreshAllCopies, sideOpIntervalMs);
        }
    }
}
