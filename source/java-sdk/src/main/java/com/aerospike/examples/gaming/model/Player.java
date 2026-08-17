package com.aerospike.examples.gaming.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Player {
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
