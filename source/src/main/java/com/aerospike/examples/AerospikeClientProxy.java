package com.aerospike.examples;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.Policy;

/**
 * A dynamic proxy that wraps an IAerospikeClient and disables all transactions
 * by setting the `txn` field to null in all policy objects.
 */
public class AerospikeClientProxy {

    /**
     * Wraps the given IAerospikeClient in a proxy that strips transactions and
     * disables commit/abort behavior.
     *
     * @param delegate the real IAerospikeClient
     * @return a proxy IAerospikeClient
     */
    public static IAerospikeClient wrap(IAerospikeClient delegate) {
        return (IAerospikeClient) Proxy.newProxyInstance(
            IAerospikeClient.class.getClassLoader(),
            new Class[]{IAerospikeClient.class},
            new TransactionStrippingHandler(delegate)
        );
    }

    /**
     * Invocation handler that removes txn references from policies and disables commit/abort.
     */
    private static class TransactionStrippingHandler implements InvocationHandler {

        private final IAerospikeClient delegate;

        public TransactionStrippingHandler(IAerospikeClient delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();

            // Disable commit(txn) and abort(txn)
            if ((methodName.equals("commit") || 
                    methodName.equals("abort"))
                    && (args != null && args.length == 1)) {
                // NOOP
                return null;
            }

            // Remove txn from any Policy arguments
            if (args != null) {
                for (Object arg : args) {
                    if (arg instanceof Policy) {
                        nullifyTxn((Policy) arg);
                    }
                }
            }

            return method.invoke(delegate, args);
        }

        /**
         * Attempts to set the txn field to null using reflection.
         */
        private void nullifyTxn(Policy policy) {
            try {
                Field txnField = policy.getClass().getField("txn");
                txnField.set(policy, null);
            } catch (NoSuchFieldException e) {
                // Field not present – skip
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Failed to nullify txn in policy: " + policy, e);
            }
        }
    }
}
