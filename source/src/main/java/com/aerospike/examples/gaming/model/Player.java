package com.aerospike.examples.gaming.model;

import com.aerospike.generator.annotations.GenExclude;
import com.aerospike.generator.annotations.GenExpression;
import com.aerospike.generator.annotations.GenMagic;
import com.aerospike.generator.annotations.GenNumber;
import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.Data;
@AerospikeRecord(namespace = "${demo.namespace:test}", set = "uccb_player")
@GenMagic
@Data
public class Player {
    @AerospikeKey
    @GenExpression("$Key")
    private int id;
    private String userName;
    private String firstName;
    private String lastName;
    private String email;
    // When the shield expires, set to 0 if no shield
    @GenExclude
    private long shieldExpiry = 0;
    // Whether this player is online or not.
    @GenExclude
    private boolean online = false;
    
    // if this player is being attacked, who are they attacked by. Set to empty string if not being attacked.
    @GenExclude
    private String beingAttackedBy = "";
    
    // Scores range from 0 to 6200
    @GenNumber(start = 0, end = 6200)
    private int score;
}
