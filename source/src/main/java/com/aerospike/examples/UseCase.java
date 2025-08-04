package com.aerospike.examples;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.mapper.tools.AeroMapper;

public interface UseCase {
    String getName();
    String getDescription();
    String getReference();
    void setup(IAerospikeClient client, AeroMapper mapper) throws Exception;
    void run(IAerospikeClient client, AeroMapper mapper) throws Exception;
    
    default String[] getTags() {
        return new String[] {};
    }
}
