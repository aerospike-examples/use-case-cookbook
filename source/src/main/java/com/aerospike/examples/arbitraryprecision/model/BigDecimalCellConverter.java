package com.aerospike.examples.arbitraryprecision.model;

import java.math.BigDecimal;

import com.aerospike.client.Value;

public class BigDecimalCellConverter implements CellConverter<BigDecimal>{
    // TODO: Customize this to how big your decimals are.
    public static final int SIZE_IN_BYTES = 20;
    @Override
    public Value toAerospike(BigDecimal data) {
        return Value.get(data.toPlainString());
    }

    @Override
    public BigDecimal fromAerospike(Object data) {
        return new BigDecimal((String)data);
    }

    @Override
    public int estimateSizeInBytes() {
        return SIZE_IN_BYTES;
    }

}
