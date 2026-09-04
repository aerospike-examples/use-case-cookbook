package com.aerospike.examples.timeseries.model;

import java.util.Date;
import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Event {
    // Id is always 25 characters: the first 13 are a timestamp, the rest make it unique
    private String id;
    private String accountId;
    private String deviceId;
    private Map<String, Object> parameters;
    private List<Integer> resolution;
    private Map<String, Object> videoMeta;
    private List<String> parameterTags;
    private String partnerId;
    private String partnerStateId;
    private Date timestamp;
}
