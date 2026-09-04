package com.aerospike.examples.recordversioning.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.generator.annotations.GenExclude;
import com.aerospike.generator.annotations.GenExpression;
import com.aerospike.generator.annotations.GenMagic;
import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.Data;

@Data
@AerospikeRecord(namespace = "test", set = "tradebase")
@GenMagic
public class TradeBase {
    
    @AerospikeKey
    @GenExpression("$Key")
    private long id;
    private String sourceSystemId;
    private int version;
    private long parentTradeId;
    private String extTradeId;
    private long contentId;
    private String book;
    private String counterparty;
    private Date tradeDate;
    private Date enteredDate;
    private Date updatedDate;
    private int tradeVersion;
    @GenExclude
    private boolean recordComplete;
    private int dataVersion;    // Version of the trade static data associated with this trade
    @GenExclude
    private Map<Long, Integer> versions = new HashMap<>();
}
