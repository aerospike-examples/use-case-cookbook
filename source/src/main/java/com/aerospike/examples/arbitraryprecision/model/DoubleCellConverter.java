package com.aerospike.examples.arbitraryprecision.model;

import com.aerospike.client.Value;

public class DoubleCellConverter implements CellConverter<Double> {

    @Override
    public Value toAerospike(Double data) {
        return Value.get((double)data);
    }

    @Override
    public Double fromAerospike(Object data) {
        return (Double)data;
    }

    @Override
    public int estimateSizeInBytes() {
        return 8;
    }
}
