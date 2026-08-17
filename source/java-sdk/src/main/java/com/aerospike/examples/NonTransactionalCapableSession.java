package com.aerospike.examples;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.policy.Behavior;

/**
 * A {@link Session} whose {@link #doInTransaction}/{@link #doInTransactionReturning} bypass real
 * Aerospike multi-record transactions entirely, running the operation once against a {@link
 * NonTransactionalSession} instead.
 * <p/>
 * Multi-record transactions require Aerospike 8+ with a strong-consistency-enabled namespace
 * (an enterprise feature) - see {@link UseCaseCookbookRunner#detectTransactionSupport}. This
 * mirrors the legacy client's {@code AerospikeClientProxy} (see ../../java), which strips {@code
 * txn} from every {@code Policy} and no-ops {@code commit}/{@code abort} for the same reason, so
 * that transaction-based use cases can still run - just without the atomicity guarantee a real
 * MRT would have provided - against any cluster.
 */
public class NonTransactionalCapableSession extends Session {

    public NonTransactionalCapableSession(Cluster cluster, Behavior behavior) {
        super(cluster, behavior);
    }

    @Override
    public void doInTransaction(TransactionalVoid operation) {
        operation.execute(new NonTransactionalSession(getCluster(), getBehavior()));
    }

    @Override
    public <T> T doInTransactionReturning(Transactional<T> operation) {
        return operation.execute(new NonTransactionalSession(getCluster(), getBehavior()));
    }
}
