package com.aerospike.examples.hotkeys;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Key;

/**
 * Key helpers for the hot-key demonstrations. The logical product id is {@code productId}; replica
 * keys append {@code :index} so traffic can be spread across multiple records.
 */
public final class HotKeyKeys {

    private HotKeyKeys() {
    }

    public static final DataSet PRODUCTS = DataSet.of(System.getProperty("demo.namespace", "test"), "uccb_hotkey");

    /** Primary (single-key) record used in the baseline simulation. */
    public static Key primary(long productId) {
        return PRODUCTS.id(productId);
    }

    /** Replica/shard key {@code productId:index}. */
    public static Key replica(long productId, int index) {
        return PRODUCTS.id(productId + ":" + index);
    }

    public static Key randomReplica(long productId, int replicaCount) {
        int index = ThreadLocalRandom.current().nextInt(replicaCount);
        return replica(productId, index);
    }

    public static List<Key> allReplicaKeys(long productId, int replicaCount) {
        List<Key> keys = new ArrayList<>(replicaCount);
        for (int i = 0; i < replicaCount; i++) {
            keys.add(replica(productId, i));
        }
        return keys;
    }

    /** Primary key followed by all replica keys - used when writes must refresh every copy. */
    public static List<Key> primaryAndAllReplicaKeys(long productId, int replicaCount) {
        List<Key> keys = new ArrayList<>(replicaCount + 1);
        keys.add(primary(productId));
        keys.addAll(allReplicaKeys(productId, replicaCount));
        return keys;
    }
}
