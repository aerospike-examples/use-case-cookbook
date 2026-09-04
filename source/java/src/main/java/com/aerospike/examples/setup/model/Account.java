package com.aerospike.examples.setup.model;

import java.util.Date;
import java.util.UUID;

import com.aerospike.generator.annotations.GenMagic;
import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@GenMagic
@AerospikeRecord(namespace = "${demo.namespace:test}", set = "uccb_account", shortName = "uccb_act")
public class Account {
    @AerospikeKey
    private UUID id;
    private String accountName;
    private int balanceInCents;
    private Date dateOpened;
}
