package com.aerospike.examples.hotkeys.model;

import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordMapper;

public class HotKeyProductMapper implements RecordMapper<HotKeyProduct> {

    @Override
    public HotKeyProduct fromMap(Map<String, Object> map, Key recordKey, int generation) {
        return new HotKeyProduct(
                (Long) map.get("id"),
                (String) map.get("sku"),
                (String) map.get("description"),
                ((Long) map.get("unitsSold")).intValue());
    }

    @Override
    public Map<String, Object> toMap(HotKeyProduct product) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", product.getId());
        map.put("sku", product.getSku());
        map.put("description", product.getDescription());
        map.put("unitsSold", product.getUnitsSold());
        return map;
    }

    @Override
    public Object id(HotKeyProduct product) {
        return product.getId();
    }
}
