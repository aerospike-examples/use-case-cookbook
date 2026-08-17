package com.aerospike.examples.transactionprocessing.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {
    public enum Status {
        APPROVED,
        DENIED,
        FRAUD
    }

    private String id;
    private long timestamp;
    private int amount;
    private String desc;
    private Status status;
    private String origin;
    private String approvalCode;
    private String accountId;
}
