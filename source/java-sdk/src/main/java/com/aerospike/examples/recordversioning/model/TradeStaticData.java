package com.aerospike.examples.recordversioning.model;

import lombok.Data;

@Data
public class TradeStaticData {
    private long tradeId;
    private int version; // The version of the content of the trade
    private String data;
    private int mutableData;
}
