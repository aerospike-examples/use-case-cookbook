package com.aerospike.examples.hotkeys;

/**
 * Shared simulation parameters for the hot-key use cases.
 */
public final class HotKeySimulationParams {

    public static final int NUM_THREADS = 25;
    public static final int DURATION_SECS = 10;
    public static final int REPLICA_COUNT = 4;

    /** Namespace transaction-pending-limit applied via info set-config for the simulation
     * (lower values make KEY_BUSY easier to observe; 0 disables the check). */
    public static final int TRANSACTION_PENDING_LIMIT = 20;

    /** Fixed logical product id for the single hot-key record used by all demonstrations. */
    public static final long HOT_PRODUCT_ID = 1L;

    /** Interval for occasional cross-traffic (writes during read load, reads during write load). */
    public static final long PERIODIC_SIDE_OP_INTERVAL_MS = 5L;

    private HotKeySimulationParams() {
    }
}
