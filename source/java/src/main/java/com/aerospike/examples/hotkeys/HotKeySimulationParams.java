package com.aerospike.examples.hotkeys;

import com.aerospike.examples.Parameter;

/**
 * Shared simulation parameters for the hot-key use cases.
 */
public final class HotKeySimulationParams {

    public static final Parameter<Integer> NUM_THREADS = new Parameter<>(
            "numThreads",
            25,
            "Number of concurrent worker threads hammering the hot key");

    public static final Parameter<Integer> DURATION_SECS = new Parameter<>(
            "durationSecs",
            10,
            "Duration of each simulation phase in seconds");

    public static final Parameter<Integer> REPLICA_COUNT = new Parameter<>(
            "replicaCount",
            4,
            "Number of replica/shard records used by the mitigation strategy");

    public static final Parameter<Integer> TRANSACTION_PENDING_LIMIT = new Parameter<>(
            "transactionPendingLimit",
            20,
            "Namespace transaction-pending-limit applied via info set-config for the simulation "
                    + "(default 20; lower values such as 5 make KEY_BUSY easier to observe; 0 disables the check)");

    /** Fixed logical product id for the single hot-key record used by all demonstrations. */
    public static final long HOT_PRODUCT_ID = 1L;

    /** Interval for occasional cross-traffic (writes during read load, reads during write load). */
    public static final long PERIODIC_SIDE_OP_INTERVAL_MS = 5L;

    private HotKeySimulationParams() {
    }
}
