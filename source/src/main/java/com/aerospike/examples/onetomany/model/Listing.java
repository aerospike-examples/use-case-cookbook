package com.aerospike.examples.onetomany.model;

import java.util.Date;

import com.aerospike.generator.annotations.GenAddress;
import com.aerospike.generator.annotations.GenAddress.AddressPart;
import com.aerospike.generator.annotations.GenExclude;
import com.aerospike.generator.annotations.GenExpression;
import com.aerospike.generator.annotations.GenMagic;
import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AerospikeRecord(namespace = "${demo.namespace:test}", set = "uccb_listing")
@GenMagic
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Listing {
    @GenExpression("'Listing-' & $Key")
    @AerospikeKey
    private String id;
    private String line1;
    private String line2;
    private String city;
    @GenAddress(AddressPart.STATE_ABBR)
    private String state;
    private String zipCode;
    private String url;
    private Date dateListed;
    @GenExclude
    private long agentId;
    private String description;
}
