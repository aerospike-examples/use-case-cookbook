package com.aerospike.examples;

public class Parameter<T> {
    private final String name;
    private final String description;
    private T value;
    
    public Parameter(String name, T value) {
        this(name, value, null);
    }
    
    public Parameter(String name, T value, String description) {
        this.name = name;
        this.value = value;
        this.description = description;
    }

    public T get() {
        return value;
    }
    
    public String getName() {
        return name;
    }
    
    public String getDescription() {
        return description;
    }
}
