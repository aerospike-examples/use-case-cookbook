package com.aerospike.examples.gaming.model;

import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordMapper;

public class PlayerMapper implements RecordMapper<Player> {

    @Override
    public Player fromMap(Map<String, Object> map, Key recordKey, int generation) {
        return new Player(
                ((Long) map.get("id")).intValue(),
                (String) map.get("userName"),
                (String) map.get("firstName"),
                (String) map.get("lastName"),
                (String) map.get("email"),
                (Long) map.get("shieldExpiry"),
                // "online" is sometimes deliberately excluded from a partial-bin operate result
                // (e.g. right after writing it, to avoid the write+read-same-bin multi-result
                // wrapper) - default to false rather than NPE-ing on the unboxed null.
                Boolean.TRUE.equals(map.get("online")),
                (String) map.get("beingAttackedBy"),
                ((Long) map.get("score")).intValue());
    }

    @Override
    public Map<String, Object> toMap(Player player) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", player.getId());
        map.put("userName", player.getUserName());
        map.put("firstName", player.getFirstName());
        map.put("lastName", player.getLastName());
        map.put("email", player.getEmail());
        map.put("shieldExpiry", player.getShieldExpiry());
        map.put("online", player.isOnline());
        map.put("beingAttackedBy", player.getBeingAttackedBy());
        map.put("score", player.getScore());
        return map;
    }

    @Override
    public Object id(Player player) {
        return player.getId();
    }
}
