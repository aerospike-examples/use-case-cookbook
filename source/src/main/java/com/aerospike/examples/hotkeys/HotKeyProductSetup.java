package com.aerospike.examples.hotkeys;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.examples.hotkeys.model.HotKeyProduct;
import com.aerospike.mapper.tools.AeroMapper;

/**
 * Seeds the shared {@link HotKeyProduct} records used by all hot-key use cases.
 */
public final class HotKeyProductSetup {

    private HotKeyProductSetup() {
    }

    public static void truncateAndSeed(IAerospikeClient client, AeroMapper mapper, int replicaCount) {
        client.truncate(null, HotKeyKeys.namespace(mapper), HotKeyKeys.setName(mapper), null);

        long productId = HotKeySimulationParams.HOT_PRODUCT_ID;
        HotKeyProduct product = new HotKeyProduct();
        product.setId(productId);
        product.setSku("SKU-1000");
        product.setDescription("Generic demo product for hot-key simulations");
        product.setUnitsSold(0);

        WritePolicy wp = client.copyWritePolicyDefault();
        wp.sendKey = true;
        Bin[] bins = productToBins(product);

        client.put(wp, HotKeyKeys.primary(mapper, productId), bins);
        for (int i = 0; i < replicaCount; i++) {
            client.put(wp, HotKeyKeys.replica(mapper, productId, i), bins);
        }
    }

    public static Bin[] productToBins(HotKeyProduct product) {
        return new Bin[] {
                new Bin("id", product.getId()),
                new Bin("sku", product.getSku()),
                new Bin("description", product.getDescription()),
                new Bin("unitsSold", product.getUnitsSold())
        };
    }

    /**
     * Batch-reads {@code unitsSold} from every shard key ({@code productId:0..N-1}) and returns the
     * sum — the logical total after sharded writes.
     */
    public static int readMergedUnitsSold(IAerospikeClient client, AeroMapper mapper, int replicaCount) {
        Key[] keys = HotKeyKeys.allReplicaKeys(mapper, HotKeySimulationParams.HOT_PRODUCT_ID, replicaCount)
                .toArray(new Key[0]);
        BatchPolicy batchPolicy = client.copyBatchPolicyDefault();
        // aerospike-client-jdk8 defaults maxConcurrentThreads to 1 (serial batch sub-requests).
        // Set to 0 so all keys in the batch are read in parallel. Not required with aerospike-client-jdk21.
        batchPolicy.maxConcurrentThreads = 0;
        com.aerospike.client.Record[] records = client.get(batchPolicy, keys, "unitsSold");
        int total = 0;
        if (records != null) {
            for (com.aerospike.client.Record record : records) {
                if (record != null) {
                    total += record.getInt("unitsSold");
                }
            }
        }
        return total;
    }

    /**
     * Increments {@code unitsSold} on the primary key and every replica in a single batch write so
     * all copies stay in sync.
     */
    public static void incrementUnitsSoldOnAllCopies(IAerospikeClient client, AeroMapper mapper,
            int replicaCount) {
        long productId = HotKeySimulationParams.HOT_PRODUCT_ID;
        Key[] keys = HotKeyKeys.primaryAndAllReplicaKeys(mapper, productId, replicaCount);
        BatchPolicy batchPolicy = client.copyBatchPolicyDefault();
        // aerospike-client-jdk8 defaults maxConcurrentThreads to 1 (serial batch sub-requests).
        // Set to 0 so all keys in the batch are written in parallel. Not required with aerospike-client-jdk21.
        batchPolicy.maxConcurrentThreads = 0;
        BatchWritePolicy batchWritePolicy = client.copyBatchWritePolicyDefault();
        client.operate(batchPolicy, batchWritePolicy, keys, Operation.add(new Bin("unitsSold", 1)));
    }
}
