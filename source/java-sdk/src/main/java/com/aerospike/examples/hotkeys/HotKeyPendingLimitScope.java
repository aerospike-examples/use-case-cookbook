package com.aerospike.examples.hotkeys;

import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.command.Info;
import com.aerospike.client.sdk.info.classes.NamespaceDetail;

/**
 * Temporarily sets {@code transaction-pending-limit} on the hot-key namespace via info
 * {@code set-config}, restoring the previous value when closed.
 */
public final class HotKeyPendingLimitScope implements AutoCloseable {

    private final Session session;
    private final String namespace;
    private final int previousLimit;
    private boolean closed;

    private HotKeyPendingLimitScope(Session session, String namespace, int previousLimit) {
        this.session = session;
        this.namespace = namespace;
        this.previousLimit = previousLimit;
    }

    /**
     * Reads the current namespace limit, applies {@code pendingLimit} on every cluster node, and
     * returns a scope that restores the original value on {@link #close()}.
     */
    public static HotKeyPendingLimitScope apply(Session session, String namespace, int pendingLimit)
            throws Exception {
        if (pendingLimit < 0) {
            throw new IllegalArgumentException("transactionPendingLimit must be >= 0 (0 disables the queue check)");
        }
        int previousLimit = readTransactionPendingLimit(session, namespace);
        setTransactionPendingLimit(session, namespace, pendingLimit);
        System.out.printf(
                "Namespace '%s' transaction-pending-limit: %d -> %d (restored to %d when the use case finishes)%n",
                namespace, previousLimit, pendingLimit, previousLimit);
        return new HotKeyPendingLimitScope(session, namespace, previousLimit);
    }

    public static int readTransactionPendingLimit(Session session, String namespace) throws Exception {
        NamespaceDetail detail = session.info().namespaceDetails(namespace)
                .orElseThrow(() -> new IllegalStateException("Namespace not found: " + namespace));
        return (int) detail.getTransactionPendingLimit();
    }

    private static void setTransactionPendingLimit(Session session, String namespace, int pendingLimit)
            throws Exception {
        String command = "set-config:context=namespace;id=" + namespace
                + ";transaction-pending-limit=" + pendingLimit;
        for (Node node : session.getCluster().getNodes()) {
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
        setTransactionPendingLimit(session, namespace, previousLimit);
        System.out.printf("Restored namespace '%s' transaction-pending-limit to %d%n", namespace, previousLimit);
    }
}
