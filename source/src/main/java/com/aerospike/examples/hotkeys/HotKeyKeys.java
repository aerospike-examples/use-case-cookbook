package com.aerospike.examples.hotkeys;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.aerospike.client.Key;
import com.aerospike.examples.hotkeys.model.HotKeyProduct;
import com.aerospike.mapper.tools.AeroMapper;

/**
 * Key helpers for the hot-key demonstrations. The logical product id is {@code productId}; replica
 * keys append {@code :index} so traffic can be spread across multiple records.
 */
public final class HotKeyKeys {

    private HotKeyKeys() {
    }

    public static String namespace(AeroMapper mapper) {
        return mapper.getNamespace(HotKeyProduct.class);
    }

    public static String setName(AeroMapper mapper) {
        return mapper.getSet(HotKeyProduct.class);
    }

    /** Primary (single-key) record used in the baseline simulation. */
    public static Key primary(AeroMapper mapper, long productId) {
        return new Key(namespace(mapper), setName(mapper), productId);
    }

    /** Replica/shard key {@code productId:index}. */
    public static Key replica(AeroMapper mapper, long productId, int index) {
        return new Key(namespace(mapper), setName(mapper), productId + ":" + index);
    }

    public static Key randomReplica(AeroMapper mapper, long productId, int replicaCount) {
        int index = ThreadLocalRandom.current().nextInt(replicaCount);
        return replica(mapper, productId, index);
    }

    public static List<Key> allReplicaKeys(AeroMapper mapper, long productId, int replicaCount) {
        List<Key> keys = new ArrayList<>(replicaCount);
        for (int i = 0; i < replicaCount; i++) {
            keys.add(replica(mapper, productId, i));
        }
        return keys;
    }

    /** Primary key followed by all replica keys — used when writes must refresh every copy. */
    public static Key[] primaryAndAllReplicaKeys(AeroMapper mapper, long productId, int replicaCount) {
        List<Key> keys = new ArrayList<>(replicaCount + 1);
        keys.add(primary(mapper, productId));
        keys.addAll(allReplicaKeys(mapper, productId, replicaCount));
        return keys.toArray(new Key[0]);
    }
}
