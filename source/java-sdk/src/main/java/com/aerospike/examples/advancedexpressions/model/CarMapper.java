package com.aerospike.examples.advancedexpressions.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordMapper;

public class CarMapper implements RecordMapper<Car> {

    @Override
    @SuppressWarnings("unchecked")
    public Car fromMap(Map<String, Object> map, Key recordKey, int generation) {
        return new Car(
                ((Long) map.get("id")).intValue(),
                (String) map.get("make"),
                (String) map.get("model"),
                ((Long) map.get("year")).intValue(),
                (String) map.get("bodyType"),
                (Double) map.get("engineSize"),
                (String) map.get("color"),
                ((Long) map.get("milage")).intValue(),
                ((Long) map.get("price")).intValue(),
                (List<String>) map.get("features"));
    }

    @Override
    public Map<String, Object> toMap(Car car) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", car.getId());
        map.put("make", car.getMake());
        map.put("model", car.getModel());
        map.put("year", car.getYear());
        map.put("bodyType", car.getBodyType());
        map.put("engineSize", car.getEngineSize());
        map.put("color", car.getColor());
        map.put("milage", car.getMilage());
        map.put("price", car.getPrice());
        map.put("features", car.getFeatures());
        return map;
    }

    @Override
    public Object id(Car car) {
        return car.getId();
    }
}
