package com.aerospike.examples;

import com.aerospike.client.sdk.Session;

public interface UseCase {
    /**
     * Get the name of this use case. This should be brief but descriptive
     */
    String getName();
    /**
     * Get a description for the use case. This should include what the use case does and potentially
     * how it does it, as this text is searchable for people trying to find a matching use case
     */
    String getDescription();
    /**
     * Get a URL reference which fully documents what the use case does and how the code behaves and
     * why the appropriate design desicions were made.
     */
    String getReference();

    /**
     * Get a list of tags / features about this use case. Defaults to empty, but is strongly
     * recommended to be populated with more details.
     */
    default String[] getTags() {
        return new String[] {};
    }

    /**
     * Get a list of the parameters for this use case to allow it to be customized
     * @return
     */
    default Parameter<?>[] getParams() {
        return new Parameter<?>[] {};
    }
    /**
     * Setup of the use case. This should be used to truncate the set, generate new data,
     * etc. This setup will be run whenever the use case is selected and does not necessarily
     * reflect business logic associated with the use case.
     *
     * @param session - The Session used to access the database. Unlike the legacy client
     * (which needed a separate IAerospikeClient and AeroMapper), the SDK's Session exposes
     * both raw record operations and object mapping.
     * @throws Exception
     */
    void setup(Session session) throws Exception;

    /**
     * Execute the use case and display results on the console. The results should be self-explanatory
     * to someone not terribly familiar with the use case, or at least documented in the URL associated
     * with the {@link #getReference()} method.
     * @param session - The Session used to access the database
     * @throws Exception
     */
    void run(Session session) throws Exception;

}