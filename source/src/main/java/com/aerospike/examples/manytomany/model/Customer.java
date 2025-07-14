package com.aerospike.examples.manytomany.model;

import java.util.Date;
import java.util.List;

import com.aerospike.generator.annotations.GenExclude;
import com.aerospike.generator.annotations.GenExpression;
import com.aerospike.generator.annotations.GenMagic;
import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.Data;
import lombok.NoArgsConstructor;

@AerospikeRecord(namespace = "${demo.namespace:test}", set = "customer")
@Data
@NoArgsConstructor
@GenMagic
public class Customer {
    @AerospikeKey
    @GenExpression("'Cust-' & $Key")
    private String custId;
    private String firstName;
    private String lastName;
    private Date dob;
    private Date dateJoined;
    @GenExclude
    private List<Account> accounts;
}
