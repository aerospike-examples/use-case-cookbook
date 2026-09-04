package com.aerospike.examples.hotkeys.model;

import com.aerospike.generator.annotations.GenExpression;
import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.Data;

/**
 * Simple product record shared by the hot-key use cases. {@link #unitsSold} is incremented by
 * write-heavy simulations; {@link #sku} and {@link #description} are returned by read-heavy
 * simulations.
 */
@Data
@AerospikeRecord(namespace = "test", set = "uccb_hotkey")
public class HotKeyProduct {

    @AerospikeKey
    @GenExpression("$Key")
    private long id;

    private String sku;
    private String description;
    private int unitsSold;
}
