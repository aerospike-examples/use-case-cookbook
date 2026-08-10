package com.aerospike.examples.hotkeys;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.mapper.tools.AeroMapper;

/**
 * Temporarily sets {@code transaction-pending-limit} on the hot-key namespace via info
 * {@code set-config}, restoring the previous value when closed.
 */
public final class HotKeyPendingLimitScope implements AutoCloseable {

    private final IAerospikeClient client;
    private final String namespace;
    private final int previousLimit;
    private boolean closed;

    private HotKeyPendingLimitScope(IAerospikeClient client, String namespace, int previousLimit) {
        this.client = client;
        this.namespace = namespace;
        this.previousLimit = previousLimit;
    }

    /**
     * Reads the current namespace limit, applies {@code pendingLimit} on every cluster node, and
     * returns a scope that restores the original value on {@link #close()}.
     */
    public static HotKeyPendingLimitScope apply(IAerospikeClient client, AeroMapper mapper, int pendingLimit)
            throws Exception {
        if (pendingLimit < 0) {
            throw new IllegalArgumentException("transactionPendingLimit must be >= 0 (0 disables the queue check)");
        }
        String namespace = HotKeyKeys.namespace(mapper);
        int previousLimit = readTransactionPendingLimit(client, namespace);
        setTransactionPendingLimit(client, namespace, pendingLimit);
        System.out.printf(
                "Namespace '%s' transaction-pending-limit: %d -> %d (restored to %d when the use case finishes)%n",
                namespace, previousLimit, pendingLimit, previousLimit);
        return new HotKeyPendingLimitScope(client, namespace, previousLimit);
    }

    public static int readTransactionPendingLimit(IAerospikeClient client, String namespace) throws Exception {
        Node[] nodes = client.getNodes();
        if (nodes.length == 0) {
            throw new IllegalStateException("No Aerospike nodes available");
        }
        return parseTransactionPendingLimit(Info.request(nodes[0],
                "get-config:context=namespace;id=" + namespace));
    }

    private static int parseTransactionPendingLimit(String configResponse) {
        for (String part : configResponse.split(";")) {
            if (part.startsWith("transaction-pending-limit=")) {
                return Integer.parseInt(part.substring("transaction-pending-limit=".length()));
            }
        }
        throw new IllegalStateException(
                "transaction-pending-limit not found in namespace config: " + configResponse);
    }

    private static void setTransactionPendingLimit(IAerospikeClient client, String namespace, int pendingLimit)
            throws Exception {
        String command = "set-config:context=namespace;id=" + namespace
                + ";transaction-pending-limit=" + pendingLimit;
        for (Node node : client.getNodes()) {
            String response = Info.request(node, command);
            if (!"ok".equalsIgnoreCase(response.trim())) {
                throw new IllegalStateException("Failed to set transaction-pending-limit on node "
                        + node.getName() + ": " + response);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        setTransactionPendingLimit(client, namespace, previousLimit);
        System.out.printf("Restored namespace '%s' transaction-pending-limit to %d%n", namespace, previousLimit);
    }
}
