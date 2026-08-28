package com.aerospike.examples.recordversioning.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.Data;

@Data
@AerospikeRecord(namespace = "test", set = "uccb_tradebase")
public class TradeBase {
    @AerospikeKey
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
    private boolean recordComplete;
    private int dataVersion; // Version of the trade static data associated with this trade
    private Map<Long, Long> versions = new HashMap<>();
}
