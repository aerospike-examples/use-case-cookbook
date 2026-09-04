package com.aerospike.examples.transactionprocessing.datamodel;

import java.util.Map;

import com.aerospike.generator.annotations.GenExpression;
import com.aerospike.generator.annotations.GenMagic;
import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.Data;

@AerospikeRecord(namespace = "${demo.namespace:test}", set = "uccb_account")
@Data
public class Account {
    @AerospikeKey
    @GenExpression("'acct-' & $Key")
    private String id;
    private Map<String, String> txns_dc1;
    private Map<String, String> txns_dc2;
}
