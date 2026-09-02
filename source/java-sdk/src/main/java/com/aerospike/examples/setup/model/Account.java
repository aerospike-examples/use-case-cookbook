package com.aerospike.examples.setup.model;

import java.util.Date;

import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@AerospikeRecord(namespace = "${demo.namespace:test}", set = "uccb_account")
public class Account {
    @AerospikeKey
    private String id;
    private String accountName;
    private int balanceInCents;
    private Date dateOpened;
}