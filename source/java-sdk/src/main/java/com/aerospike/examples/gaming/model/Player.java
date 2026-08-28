package com.aerospike.examples.gaming.model;

import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@AerospikeRecord(namespace = "test", set = "uccb_player")
public class Player {
    @AerospikeKey
    private int id;
    private String userName;
    private String firstName;
    private String lastName;
    private String email;
    private long shieldExpiry;
    private boolean online;
    private String beingAttackedBy;
    private int score;
}
