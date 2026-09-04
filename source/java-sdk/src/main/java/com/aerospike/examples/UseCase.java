package com.aerospike.examples;

import com.aerospike.client.sdk.Session;
import com.aerospike.mapper.tools.AeroMapper;

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
     * @param session - The Session used to access the database.
     * @param mapper - The registered AeroMapper, so use cases can derive their TypedDataSets
     * (namespace, set) from each model's own {@code @AerospikeRecord} annotation via {@link
     * AeroMapper#getTypedDataSet(Class)} rather than hardcoding namespace/set strings a second
     * time - keeping the annotation as the single source of truth (including
     * {@code -Ddemo.namespace} overrides expressed as a {@code ${demo.namespace:test}} placeholder
     * in the annotation's {@code namespace} attribute).
     * @throws Exception
     */
    void setup(Session session, AeroMapper mapper) throws Exception;

    /**
     * Execute the use case and display results on the console. The results should be self-explanatory
     * to someone not terribly familiar with the use case, or at least documented in the URL associated
     * with the {@link #getReference()} method.
     * @param session - The Session used to access the database
     * @param mapper - The registered AeroMapper - see {@link #setup} for why this is passed
     * @throws Exception
     */
    void run(Session session, AeroMapper mapper) throws Exception;

}