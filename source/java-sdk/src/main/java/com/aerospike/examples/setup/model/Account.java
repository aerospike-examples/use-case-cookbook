package com.aerospike.examples.setup.model;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Account {
    private String id;
    private String accountName;
    private int balanceInCents;
    private Date dateOpened;
}