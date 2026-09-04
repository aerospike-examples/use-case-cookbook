package com.aerospike.examples.hotkeys.model;

import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Simple product record shared by the hot-key use cases. {@link #unitsSold} is incremented by
 * write-heavy simulations; {@link #sku} and {@link #description} are returned by read-heavy
 * simulations.
 * <p/>
 * Only the primary (single-key) record maps onto this class via {@code @AerospikeKey} - replica
 * keys use a custom {@code productId:index} id (see {@link com.aerospike.examples.hotkeys.HotKeyKeys}),
 * which object mapping can't derive, so those are still written as raw bins.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@AerospikeRecord(namespace = "${demo.namespace:test}", set = "uccb_hotkey")
public class HotKeyProduct {
    @AerospikeKey
    private long id;
    private String sku;
    private String description;
    private int unitsSold;
}
