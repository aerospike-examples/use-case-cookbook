package com.aerospike.examples.recordversioning.model;

import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.Data;

@Data
@AerospikeRecord(namespace = "${demo.namespace:test}", set = "uccb_tradecontent")
public class TradeStaticData {
    @AerospikeKey
    private long tradeId;
    private int version; // The version of the content of the trade
    private String data;
    private int mutableData;
}
