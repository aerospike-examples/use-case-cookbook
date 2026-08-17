package com.aerospike.examples.timeseries.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A purely in-memory concept used to drive sample-data generation - accounts are never persisted
 * as their own Aerospike record in this use case, only their events are (see {@link Event}).
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Account {
    private String id;
    private int numDevices;
}
