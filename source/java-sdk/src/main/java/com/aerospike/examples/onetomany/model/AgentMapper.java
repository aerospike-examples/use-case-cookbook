package com.aerospike.examples.onetomany.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordMapper;

public class AgentMapper implements RecordMapper<Agent> {

    @Override
    public Agent fromMap(Map<String, Object> map, Key recordKey, int generation) {
        Object regDate = map.get("regDate");
        return new Agent(
                (Long) map.get("agentId"),
                (String) map.get("firstName"),
                (String) map.get("lastName"),
                (String) map.get("email"),
                (String) map.get("phoneNum"),
                regDate == null ? null : new Date((long) regDate));
    }

    @Override
    public Map<String, Object> toMap(Agent agent) {
        Map<String, Object> map = new HashMap<>();
        map.put("agentId", agent.getAgentId());
        map.put("firstName", agent.getFirstName());
        map.put("lastName", agent.getLastName());
        map.put("email", agent.getEmail());
        map.put("phoneNum", agent.getPhoneNum());
        map.put("regDate", agent.getRegDate() == null ? null : agent.getRegDate().getTime());
        return map;
    }

    @Override
    public Object id(Agent agent) {
        return agent.getAgentId();
    }
}
