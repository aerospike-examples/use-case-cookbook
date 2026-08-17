package com.aerospike.examples.manytomany.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordMapper;

public class CustomerMapper implements RecordMapper<Customer> {

    @Override
    public Customer fromMap(Map<String, Object> map, Key recordKey, int generation) {
        Object dob = map.get("dob");
        Object dateJoined = map.get("dateJoined");
        return new Customer(
                (String) map.get("custId"),
                (String) map.get("firstName"),
                (String) map.get("lastName"),
                dob == null ? null : new Date((long) dob),
                dateJoined == null ? null : new Date((long) dateJoined));
    }

    @Override
    public Map<String, Object> toMap(Customer customer) {
        Map<String, Object> map = new HashMap<>();
        map.put("custId", customer.getCustId());
        map.put("firstName", customer.getFirstName());
        map.put("lastName", customer.getLastName());
        map.put("dob", customer.getDob() == null ? null : customer.getDob().getTime());
        map.put("dateJoined", customer.getDateJoined() == null ? null : customer.getDateJoined().getTime());
        return map;
    }

    @Override
    public Object id(Customer customer) {
        return customer.getCustId();
    }
}
