package com.aerospike.examples.manytomany.model;

import java.util.Date;

import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@AerospikeRecord(namespace = "test", set = "uccb_customer")
public class Customer {
    @AerospikeKey
    private String custId;
    private String firstName;
    private String lastName;
    private Date dob;
    private Date dateJoined;
}
