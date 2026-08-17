package com.aerospike.examples.manytomany.model;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Customer {
    private String custId;
    private String firstName;
    private String lastName;
    private Date dob;
    private Date dateJoined;
}
