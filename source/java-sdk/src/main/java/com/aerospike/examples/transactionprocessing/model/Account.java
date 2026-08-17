package com.aerospike.examples.transactionprocessing.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The account's recent-transactions maps ({@code txns_dc1}/{@code txns_dc2}) are managed as raw
 * CDT bins directly (see {@code TopTransactionsAcrossDcs}), not through object mapping - they
 * aren't modeled as fields here.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Account {
    private String id;
}
