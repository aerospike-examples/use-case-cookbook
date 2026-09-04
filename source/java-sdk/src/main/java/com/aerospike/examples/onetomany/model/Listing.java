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
@AerospikeRecord(namespace = "${demo.namespace:test}", set = "uccb_listing")
public class Listing {
    @AerospikeKey
    private String id;
    private String line1;
    private String line2;
    private String city;
    private String state;
    private String zipCode;
    private String url;
    private Date dateListed;
    private long agentId;
    private String description;
}
