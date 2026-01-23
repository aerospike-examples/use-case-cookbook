package com.aerospike.examples.arbitraryprecision.model;

import com.aerospike.client.Value;

public interface CellConverter<T> {
    Value toAerospike(T data);
    T fromAerospike(Object data);
    int estimateSizeInBytes();
}
