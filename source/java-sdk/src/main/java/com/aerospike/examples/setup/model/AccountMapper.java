package com.aerospike.examples.setup.model;

import java.util.Date;
import java.util.Map;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordMapper;

public class AccountMapper implements RecordMapper<Account> {

    @Override
    public Account fromMap(Map<String, Object> map, Key recordKey, int generation) {
        return new Account(
                (String) map.get("id"),
                (String) map.get("accountName"),
                ((Long) map.get("balanceInCents")).intValue(),
                new Date((long) map.get("dateOpened")));
    }

    @Override
    public Map<String, Object> toMap(Account account) {
        return Map.of(
                "id", account.getId(),
                "accountName", account.getAccountName(),
                "balanceInCents", account.getBalanceInCents(),
                "dateOpened", account.getDateOpened().getTime());
    }

    @Override
    public Object id(Account account) {
        return account.getId();
    }
}
