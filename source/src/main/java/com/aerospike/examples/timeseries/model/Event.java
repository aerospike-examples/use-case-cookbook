package com.aerospike.examples.timeseries.model;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.aerospike.generator.annotations.GenDate;
import com.aerospike.generator.annotations.GenExpression;
import com.aerospike.generator.annotations.GenList;
import com.aerospike.generator.annotations.GenOneOf;
import com.aerospike.generator.annotations.GenString;
import com.aerospike.generator.annotations.GenString.StringType;
import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.Data;
import lombok.NoArgsConstructor;

@AerospikeRecord(namespace = "${demo.namespace:test}", set = "events")
@Data
@NoArgsConstructor
public class Event {
    // Id is always 25 characters, the first part (13 chars) of
    // which are a timestamp, the rest makes it unique
    @AerospikeKey
    @GenExpression("$Key")
    private String id;
    private String accountId;
    private String deviceId;
    private Map<String, Object> parameters;
    @GenList(items = 2)
    private List<Integer> resolution;
    private Map<String, Object> videoMeta;
    @GenList(minItems = 1, maxItems = 5)
    private List<String> parameterTags;
    @GenString(type = StringType.CHARACTERS, length = 20)
    private String partnerId;
    @GenString(type = StringType.WORDS, length = 3)
    private String partnerStateId;
    @GenDate(start = "now - 14d", end = "now")
    private Date timestamp;
    @GenOneOf("activity,motion,snapshot")
    private String type;
}
