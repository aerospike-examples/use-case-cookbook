package com.aerospike.examples.arbitraryprecision.model;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CubeUpdates<T> {
    private ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, T>> changes = new ConcurrentHashMap<>();
    
    public void updateCell(int row, int col, T value) {
        changes.computeIfAbsent(row, r -> new ConcurrentHashMap<>())
            .put(col, value);
    }

    public void updateRow(int row, List<T> values) {
        var rowMap = changes.computeIfAbsent(row, r -> new ConcurrentHashMap<>());
        for (int i = 0; i < values.size(); i++) {
            rowMap.put(i, values.get(i));
        }
    }
    
    /**
     * Get all rows that have changes
     */
    public Set<Integer> getAffectedRows() {
        return changes.keySet();
    }
    
    /**
     * Get all changes for a specific row
     */
    public Map<Integer, T> getRowChanges(int row) {
        return changes.get(row);
    }
    
    /**
     * Check if there are any changes
     */
    public boolean hasChanges() {
        return !changes.isEmpty();
    }
    
}
