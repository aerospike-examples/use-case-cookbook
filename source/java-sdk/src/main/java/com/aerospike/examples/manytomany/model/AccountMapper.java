package com.aerospike.examples.manytomany.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordMapper;

public class AccountMapper implements RecordMapper<Account> {

    @Override
    public Account fromMap(Map<String, Object> map, Key recordKey, int generation) {
        Object dateOpened = map.get("dateOpened");
        return new Account(
                (String) map.get("id"),
                (String) map.get("accountName"),
                ((Long) map.get("balanceInCents")).intValue(),
                dateOpened == null ? null : new Date((long) dateOpened));
    }

    @Override
    public Map<String, Object> toMap(Account account) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", account.getId());
        map.put("accountName", account.getAccountName());
        map.put("balanceInCents", account.getBalanceInCents());
        map.put("dateOpened", account.getDateOpened() == null ? null : account.getDateOpened().getTime());
        return map;
    }

    @Override
    public Object id(Account account) {
        return account.getId();
    }
}
