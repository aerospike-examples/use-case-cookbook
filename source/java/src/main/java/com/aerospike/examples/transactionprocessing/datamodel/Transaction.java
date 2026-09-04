package com.aerospike.examples.transactionprocessing.datamodel;

import com.aerospike.generator.annotations.GenExpression;
import com.aerospike.generator.annotations.GenHexString;
import com.aerospike.generator.annotations.GenMagic;
import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AerospikeRecord(namespace = "${demo.namespace:test}", set = "uccb_txn")
@Data
@GenMagic
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {
    public enum Status {
        APPROVED,
        DENIED,
        FRAUD
    }
    @AerospikeKey
    @GenExpression("'txn-' & $Key")
    private String id;
    private long timestamp;
    private int amount;
    private String desc;
    private Status status;
    private String origin;
    @GenHexString(length = 12)
    private String approvalCode;
    @GenExpression("'acct-'& @GenNumber(start = 1, end = $NUM_ACCOUNTS)")
    private String accountId;
}
