package com.aerospike.examples.recordversioning.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import lombok.Data;

@Data
public class TradeBase {
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
