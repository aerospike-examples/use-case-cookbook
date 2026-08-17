package com.aerospike.examples.hotkeys.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Simple product record shared by the hot-key use cases. {@link #unitsSold} is incremented by
 * write-heavy simulations; {@link #sku} and {@link #description} are returned by read-heavy
 * simulations.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class HotKeyProduct {
    private long id;
    private String sku;
    private String description;
    private int unitsSold;
}
