package com.aerospike.examples.recordversioning.model;

import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordMapper;

public class TradeStaticDataMapper implements RecordMapper<TradeStaticData> {

    @Override
    public TradeStaticData fromMap(Map<String, Object> map, Key recordKey, int generation) {
        TradeStaticData data = new TradeStaticData();
        data.setTradeId((Long) map.get("tradeId"));
        data.setVersion(((Long) map.get("version")).intValue());
        data.setData((String) map.get("data"));
        data.setMutableData(((Long) map.get("mutableData")).intValue());
        return data;
    }

    @Override
    public Map<String, Object> toMap(TradeStaticData data) {
        Map<String, Object> map = new HashMap<>();
        map.put("tradeId", data.getTradeId());
        map.put("version", data.getVersion());
        map.put("data", data.getData());
        map.put("mutableData", data.getMutableData());
        return map;
    }

    @Override
    public Object id(TradeStaticData data) {
        return data.getTradeId();
    }
}
