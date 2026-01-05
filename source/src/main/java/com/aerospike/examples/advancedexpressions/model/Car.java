package com.aerospike.examples.advancedexpressions.model;

import java.util.List;

import com.aerospike.generator.annotations.GenExpression;
import com.aerospike.generator.annotations.GenList;
import com.aerospike.generator.annotations.GenMagic;
import com.aerospike.generator.annotations.GenNumber;
import com.aerospike.generator.annotations.GenOneOf;
import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.Data;

@AerospikeRecord(namespace = "${demo.namespace:test}", set = "uccb_car")
@GenMagic
@Data
public class Car {
    public enum BodyType { SEDAN, SUV, HATCHBACK, COUPE, CONVERTIBLE, UTE}
    public enum TransmissionType { AUTOMATIC, MANUAL, CVT, DUAL_CLUTCH }
    @AerospikeKey
    @GenExpression("$Key")
    private int id;
    @GenOneOf("Toyota,Holden,Mitsubishi,Kia,Ferrari,Volvo,Audi,Datsun,Suzuki")
    private String make;
    @GenOneOf("Corolla,Commodore,Outlander,Sorento,Swift,488 Spider,XC90")
    private String model;
    @GenNumber(start = 2000, end = 2025)
    private int year;
    private BodyType bodyType;
    @GenNumber(start = 7, end = 80, divisor = 10)
    private double engineSize;
    private String color;
    private int milage;
    private int price;
    @GenList(minItems = 0, maxItems = 8, stringOptions = "Bluetooth Connectivity, USB Charging Ports, Apple CarPlay, Android Auto, Heated Seats, "
            + "Ventilated Seats, Sunroof, Panoramic Roof, Navigation System, Keyless Entry, Push Button Start, Remote Start, "
            + "Adaptive Cruise Control, Blind Spot Monitoring, Lane Departure Warning, Automatic Emergency Braking, "
            + "Parking Sensors, Rearview Camera, 360-Degree Camera, Leather Upholstery, Wireless Charging, Heads-Up Display, "
            + "Premium Sound System, LED Headlights, Rain-Sensing Wipers, Heated Steering Wheel, "
            + "Power Liftgate, Roof Rails, Tow Package, Ambient Interior Lighting"
    )
    private List<String> features;
}
