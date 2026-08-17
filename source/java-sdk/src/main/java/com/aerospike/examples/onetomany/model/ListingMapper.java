package com.aerospike.examples.onetomany.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordMapper;

public class ListingMapper implements RecordMapper<Listing> {

    @Override
    public Listing fromMap(Map<String, Object> map, Key recordKey, int generation) {
        Object dateListed = map.get("dateListed");
        return new Listing(
                (String) map.get("id"),
                (String) map.get("line1"),
                (String) map.get("line2"),
                (String) map.get("city"),
                (String) map.get("state"),
                (String) map.get("zipCode"),
                (String) map.get("url"),
                dateListed == null ? null : new Date((long) dateListed),
                (Long) map.get("agentId"),
                (String) map.get("description"));
    }

    @Override
    public Map<String, Object> toMap(Listing listing) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", listing.getId());
        map.put("line1", listing.getLine1());
        map.put("line2", listing.getLine2());
        map.put("city", listing.getCity());
        map.put("state", listing.getState());
        map.put("zipCode", listing.getZipCode());
        map.put("url", listing.getUrl());
        map.put("dateListed", listing.getDateListed() == null ? null : listing.getDateListed().getTime());
        map.put("agentId", listing.getAgentId());
        map.put("description", listing.getDescription());
        return map;
    }

    @Override
    public Object id(Listing listing) {
        return listing.getId();
    }
}
