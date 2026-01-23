package com.aerospike.examples.arbitraryprecision.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Data;

@Data
public class DataCube<T> {
    private int version;
    private int columns;
    private int rows;
    private Map<Long, Integer> versions = new HashMap<>();
    
    private transient List<List<T>> cubeData;
    
    public static final String[] NON_VERSIONS_BINS = new String[] {"version", "columns", "rows"};

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append( "DataCube [version=" + version + ", columns=" + columns + ", rows=" + rows + ", versions=" + versions);
        if (cubeData != null) {
            for (int i = 0; i < cubeData.size(); i++) {
                sb.append(String.format("\n%05d: %s",i, cubeData.get(i)));
            }
        }
        return sb.toString();
    }
}
