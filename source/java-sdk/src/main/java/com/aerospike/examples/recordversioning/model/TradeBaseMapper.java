package com.aerospike.examples.recordversioning.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordMapper;

public class TradeBaseMapper implements RecordMapper<TradeBase> {

    @Override
    @SuppressWarnings("unchecked")
    public TradeBase fromMap(Map<String, Object> map, Key recordKey, int generation) {
        TradeBase trade = new TradeBase();
        trade.setId((Long) map.get("id"));
        trade.setSourceSystemId((String) map.get("sourceSystemId"));
        trade.setVersion(((Long) map.get("version")).intValue());
        trade.setParentTradeId((Long) map.get("parentTradeId"));
        trade.setExtTradeId((String) map.get("extTradeId"));
        trade.setContentId((Long) map.get("contentId"));
        trade.setBook((String) map.get("book"));
        trade.setCounterparty((String) map.get("counterparty"));
        trade.setTradeDate(toDate(map.get("tradeDate")));
        trade.setEnteredDate(toDate(map.get("enteredDate")));
        trade.setUpdatedDate(toDate(map.get("updatedDate")));
        trade.setTradeVersion(((Long) map.get("tradeVersion")).intValue());
        trade.setRecordComplete(Boolean.TRUE.equals(map.get("recordComplete")));
        trade.setDataVersion(((Long) map.get("dataVersion")).intValue());
        Object versions = map.get("versions");
        if (versions != null) {
            trade.setVersions(new HashMap<>((Map<Long, Long>) versions));
        }
        return trade;
    }

    private static Date toDate(Object value) {
        return value == null ? null : new Date((Long) value);
    }

    @Override
    public Map<String, Object> toMap(TradeBase trade) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", trade.getId());
        map.put("sourceSystemId", trade.getSourceSystemId());
        map.put("version", trade.getVersion());
        map.put("parentTradeId", trade.getParentTradeId());
        map.put("extTradeId", trade.getExtTradeId());
        map.put("contentId", trade.getContentId());
        map.put("book", trade.getBook());
        map.put("counterparty", trade.getCounterparty());
        map.put("tradeDate", trade.getTradeDate() == null ? null : trade.getTradeDate().getTime());
        map.put("enteredDate", trade.getEnteredDate() == null ? null : trade.getEnteredDate().getTime());
        map.put("updatedDate", trade.getUpdatedDate() == null ? null : trade.getUpdatedDate().getTime());
        map.put("tradeVersion", trade.getTradeVersion());
        map.put("recordComplete", trade.isRecordComplete());
        map.put("dataVersion", trade.getDataVersion());
        map.put("versions", trade.getVersions());
        return map;
    }

    @Override
    public Object id(TradeBase trade) {
        return trade.getId();
    }
}
