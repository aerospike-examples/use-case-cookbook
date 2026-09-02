package com.aerospike.examples.hotkeys;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Session;
import com.aerospike.examples.hotkeys.model.HotKeyProduct;

/**
 * Seeds the shared {@link HotKeyProduct} records used by all hot-key use cases.
 */
public final class HotKeyProductSetup {

    private HotKeyProductSetup() {
    }

    public static void truncateAndSeed(Session session, int replicaCount) {
        DataSet products = HotKeyKeys.PRODUCTS;
        session.truncate(products);

        long productId = HotKeySimulationParams.HOT_PRODUCT_ID;
        HotKeyProduct product = new HotKeyProduct(productId, "SKU-1000", "Generic demo product for hot-key simulations", 0);

        // Replica keys use a custom "productId:index" id, not product.getId() - so these are raw bin
        // writes rather than session.upsert(dataSet).object(product), which derives the key from the
        // mapper's id() instead.
        writeProductBins(session, HotKeyKeys.primary(productId), product);
        for (int i = 0; i < replicaCount; i++) {
            writeProductBins(session, HotKeyKeys.replica(productId, i), product);
        }
    }

    private static void writeProductBins(Session session, Key key, HotKeyProduct product) {
        session.upsert(key)
                .bin("id").setTo(product.getId())
                .bin("sku").setTo(product.getSku())
                .bin("description").setTo(product.getDescription())
                .bin("unitsSold").setTo((long) product.getUnitsSold())
                .execute();
    }

    /** Reads {@code unitsSold} from a single key, failing clearly if the record isn't seeded yet. */
    public static int readUnitsSold(Session session, Key key) {
        Record record = session.query(key).execute().getFirstRecord();
        if (record == null) {
            throw new IllegalStateException(
                    "No product record at key " + key + " - run setup() before run().");
        }
        return record.getInt("unitsSold");
    }

    /**
     * Batch-reads {@code unitsSold} from every shard key ({@code productId:0..N-1}) and returns the
     * sum - the logical total after sharded writes.
     */
    public static int readMergedUnitsSold(Session session, int replicaCount) {
        List<Key> keys = HotKeyKeys.allReplicaKeys(HotKeySimulationParams.HOT_PRODUCT_ID, replicaCount);
        AtomicInteger total = new AtomicInteger();
        try (RecordStream stream = session.query(keys).readingOnlyBins("unitsSold").execute()) {
            stream.forEach(result -> {
                if (result.isOk()) {
                    total.addAndGet(result.recordOrThrow().getInt("unitsSold"));
                }
            });
        }
        return total.get();
    }

    /**
     * Increments {@code unitsSold} on the primary key and every replica in a single batch write so
     * all copies stay in sync.
     */
    public static void incrementUnitsSoldOnAllCopies(Session session, int replicaCount) {
        List<Key> keys = HotKeyKeys.primaryAndAllReplicaKeys(HotKeySimulationParams.HOT_PRODUCT_ID, replicaCount);
        session.upsert(keys).bin("unitsSold").add(1).execute();
    }
}
