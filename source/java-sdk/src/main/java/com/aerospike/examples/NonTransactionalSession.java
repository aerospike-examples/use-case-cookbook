package com.aerospike.examples;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.TransactionalSession;
import com.aerospike.client.sdk.command.Txn;
import com.aerospike.client.sdk.policy.Behavior;

/**
 * A {@link TransactionalSession} that never attaches a real Aerospike transaction to its
 * operations, by always reporting {@link #getCurrentTransaction()} as {@code null}. See
 * {@link NonTransactionalCapableSession} for why this exists.
 * <p/>
 * Also overrides {@code doInTransaction}/{@code doInTransactionReturning} to run the operation
 * directly against {@code this} rather than delegating to {@link TransactionalSession}'s own
 * (real-transaction) implementation. This matters for use cases like {@code Leaderboard} that
 * take a {@code Session} parameter and call {@code session.doInTransaction(...)} internally: if
 * they're invoked with one of these sessions as that parameter (i.e. from inside another
 * transaction, nesting), the nested call must stay a no-op wrapper too - otherwise it would try to
 * commit/roll-forward a real {@code Txn} that no operation ever actually attached itself to
 * (since {@link #getCurrentTransaction()} always returns {@code null}), which fails.
 */
class NonTransactionalSession extends TransactionalSession {

    protected NonTransactionalSession(Cluster cluster, Behavior behavior) {
        super(cluster, behavior);
    }

    @Override
    public Txn getCurrentTransaction() {
        return null;
    }

    @Override
    public void doInTransaction(TransactionalVoid operation) {
        operation.execute(this);
    }

    @Override
    public <T> T doInTransactionReturning(Transactional<T> operation) {
        return operation.execute(this);
    }
}
