package com.aerospike.examples.advancedexpressions.model;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Car {
    public enum BodyType {
        SEDAN, SUV, HATCHBACK, COUPE, CONVERTIBLE, UTE
    }

    private int id;
    private String make;
    private String model;
    private int year;
    private String bodyType;
    private double engineSize;
    private String color;
    private int milage;
    private int price;
    private List<String> features;
}
