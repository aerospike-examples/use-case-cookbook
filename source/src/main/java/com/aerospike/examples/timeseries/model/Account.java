package com.aerospike.examples.timeseries.model;

import com.aerospike.generator.annotations.GenExpression;
import com.aerospike.generator.annotations.GenNumber;

import lombok.Data;

@Data
public class Account {
    @GenExpression("'acct-' & $Key)")
    private String id;
    
    @GenNumber(start = 1, end = 20)
    private int numDevices;
}
