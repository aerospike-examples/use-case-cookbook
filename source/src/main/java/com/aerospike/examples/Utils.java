package com.aerospike.examples;

import java.util.concurrent.ThreadLocalRandom;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Txn;

public class Utils {
    /**
     * Sleep for the specified number of milliseconds without throwing a checked exception
     * @param milliseconds - the number of milliseconds to sleep for.
     */
    public static void sleep(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Sleep a random number of milliseconds
     * @param minMs - the lower bound of the millisecond range (inclusive)
     * @param maxMs - the upper bound of the millisecond range (inclusive)
     */
    public static void sleep(int minMs, int maxMs) {
        sleep(ThreadLocalRandom.current().nextInt(minMs, maxMs+1));
    }
    @FunctionalInterface
    public static interface Transactional<T> {
        T execute(Txn txn);
    }
    
    // Functional interface for void-returning operations
    @FunctionalInterface
    public static interface TransactionalVoid {
        void execute(Txn txn);
    }
    
    private static boolean isRetryableResultCode(int resultCode) {
        switch (resultCode) {
        case ResultCode.MRT_BLOCKED:
        case ResultCode.MRT_EXPIRED:
        case ResultCode.MRT_VERSION_MISMATCH:
        case ResultCode.TXN_FAILED:
            return true;
        }
        return false;
    }

    public static <T> T doInTransaction(IAerospikeClient client, Transactional<T> operation) {
        while (true) {
            Txn txn = new Txn();
            try {
                T result = operation.execute(txn);
                client.commit(txn);
                return result;
            }
            catch (AerospikeException ae) {
                client.abort(txn);
                if (isRetryableResultCode(ae.getResultCode())) {
                    // These can be retried from the beginning
                    sleep(1,5);
                }
                else {
                    // These cannot be retried
                    throw ae;
                }
            }
            catch (Exception e) {
                client.abort(txn);
                throw e;
            }
        }
    }
    
    public static void doInTransaction(IAerospikeClient client, TransactionalVoid operation) {
        while (true) {
            Txn txn = new Txn();
            try {
                operation.execute(txn);
                client.commit(txn);
                break;
            }
            catch (AerospikeException ae) {
                client.abort(txn);
                if (isRetryableResultCode(ae.getResultCode())) {
                    // These can be retried from the beginning
                    sleep(1,5);
                }
                else {
                    // These cannot be retried
                    throw ae;
                }
            }
            catch (Exception e) {
                client.abort(txn);
                throw e;
            }
        }
    }}
