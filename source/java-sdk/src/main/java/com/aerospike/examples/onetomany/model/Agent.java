package com.aerospike.examples.onetomany.model;

import java.util.Date;

import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@AerospikeRecord(namespace = "${demo.namespace:test}", set = "uccb_agent")
public class Agent {
    @AerospikeKey
    private long agentId;
    private String firstName;
    private String lastName;
    private String email;
    private String phoneNum;
    private Date regDate;
}
