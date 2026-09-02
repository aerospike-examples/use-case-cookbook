package com.aerospike.examples.hotkeys;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.TypedDataSet;
import com.aerospike.examples.hotkeys.model.HotKeyProduct;

/**
 * Key helpers for the hot-key demonstrations. The logical product id is {@code productId}; replica
 * keys append {@code :index} so traffic can be spread across multiple records.
 */
public final class HotKeyKeys {

    private HotKeyKeys() {
    }

    public static final TypedDataSet<HotKeyProduct> PRODUCTS =
            TypedDataSet.of(System.getProperty("demo.namespace", "test"), "uccb_hotkey", HotKeyProduct.class);

    /**
     * Primary (single-key) record used in the baseline simulation. Returns a raw {@link Key}
     * (rather than {@code TypedKey<HotKeyProduct>}) so it stays a drop-in replacement everywhere
     * it's already used for raw bin operations - {@link HotKeyProductSetup#truncateAndSeed} is the
     * one place that needs the typed key directly, for {@code object(product)}.
     */
    public static Key primary(long productId) {
        return PRODUCTS.id(productId).getKey();
    }

    /** Replica/shard key {@code productId:index}. */
    public static Key replica(long productId, int index) {
        return PRODUCTS.id(productId + ":" + index).getKey();
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
