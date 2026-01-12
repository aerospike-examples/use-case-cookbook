package com.aerospike.examples.recordversioning.model;

import com.aerospike.generator.annotations.GenString;
import com.aerospike.generator.annotations.GenString.StringType;
import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.Data;

@Data
@AerospikeRecord(namespace = "test", set = "tradecontent")
public class TradeStaticData {
    @AerospikeKey
    private long tradeId;
    private int version;       // The version of the content of the trade. 
    @GenString(type = StringType.WORDS, minLength = 500, maxLength = 2000)
    private String data;
    private int mutableData;
}
