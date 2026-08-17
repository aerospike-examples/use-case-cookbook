package com.aerospike.examples.transactionprocessing.model;

import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordMapper;
import com.aerospike.examples.transactionprocessing.model.Transaction.Status;

public class TransactionMapper implements RecordMapper<Transaction> {

    @Override
    public Transaction fromMap(Map<String, Object> map, Key recordKey, int generation) {
        return new Transaction(
                (String) map.get("id"),
                (Long) map.get("timestamp"),
                ((Long) map.get("amount")).intValue(),
                (String) map.get("desc"),
                Status.valueOf((String) map.get("status")),
                (String) map.get("origin"),
                (String) map.get("approvalCode"),
                (String) map.get("accountId"));
    }

    @Override
    public Map<String, Object> toMap(Transaction transaction) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", transaction.getId());
        map.put("timestamp", transaction.getTimestamp());
        map.put("amount", transaction.getAmount());
        map.put("desc", transaction.getDesc());
        map.put("status", transaction.getStatus().name());
        map.put("origin", transaction.getOrigin());
        map.put("approvalCode", transaction.getApprovalCode());
        map.put("accountId", transaction.getAccountId());
        return map;
    }

    @Override
    public Object id(Transaction transaction) {
        return transaction.getId();
    }
}
