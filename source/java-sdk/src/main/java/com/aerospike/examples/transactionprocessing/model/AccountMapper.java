package com.aerospike.examples.transactionprocessing.model;

import java.util.Map;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordMapper;

public class AccountMapper implements RecordMapper<Account> {

    @Override
    public Account fromMap(Map<String, Object> map, Key recordKey, int generation) {
        return new Account((String) map.get("id"));
    }

    @Override
    public Map<String, Object> toMap(Account account) {
        return Map.of("id", account.getId());
    }

    @Override
    public Object id(Account account) {
        return account.getId();
    }
}
