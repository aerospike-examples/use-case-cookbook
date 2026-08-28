package com.aerospike.examples.transactionprocessing.model;

import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@AerospikeRecord(namespace = "test", set = "uccb_txn")
public class Transaction {
    public enum Status {
        APPROVED,
        DENIED,
        FRAUD
    }

    @AerospikeKey
    private String id;
    private long timestamp;
    private int amount;
    private String desc;
    private Status status;
    private String origin;
    private String approvalCode;
    private String accountId;
}
