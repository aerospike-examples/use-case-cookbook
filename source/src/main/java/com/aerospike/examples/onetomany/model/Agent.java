package com.aerospike.examples.onetomany.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.aerospike.generator.annotations.GenExclude;
import com.aerospike.generator.annotations.GenExpression;
import com.aerospike.generator.annotations.GenMagic;
import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.Data;

@AerospikeRecord(namespace = "${demo.namespace:test}", set = "agent")
@Data
@GenMagic
public class Agent {
    @GenExpression("$Key")
    @AerospikeKey
    private long agentId;
    private String firstName;
    private String lastName;
    private String email;
    private String phoneNum;
    private Date regDate;
    @GenExclude
    private List<Listing> listings;
    
    public Agent() {
        this.listings = new ArrayList<>();
    }
}
