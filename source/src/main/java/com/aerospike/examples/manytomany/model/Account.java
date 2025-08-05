package com.aerospike.examples.manytomany.model;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.aerospike.generator.annotations.GenExclude;
import com.aerospike.generator.annotations.GenMagic;
import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.Data;

@Data
@GenMagic
@AerospikeRecord(namespace = "${demo.namespace:test}", set = "uccb_account")
public class Account {
    @AerospikeKey
    private UUID id;
    private String accountName;
    private int balanceInCents;
    private Date dateOpened;
    @GenExclude
    private List<Customer> owners;
}
