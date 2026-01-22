package com.aerospike.examples.recordversioning;

import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Txn;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.examples.Async;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.Utils;
import com.aerospike.examples.recordversioning.model.TradeBase;
import com.aerospike.examples.recordversioning.model.TradeStaticData;
import com.aerospike.generator.Generator;
import com.aerospike.generator.ValueCreator;
import com.aerospike.mapper.tools.AeroMapper;

public class VersioningRecords implements UseCase{
    public static interface ChangeHandler<T> {
        List<Operation> apply(Record existingItem);
    }

    private final MapPolicy MAP_POLICY = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
    @Override
    public String getName() {
        return "Versioned Records";
    }

    @Override
    public String getDescription() {
        return "Maintain historical versions of records with point-in-time query capabilities. Demonstrates atomic "
              + "version creation using transactions and time-based queries using map operations. Objects are assumed "
              + "to have 2 parts -- a base record which changes frequently and is small, and a details record which is "
              + "large and changes infrequently.";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/versioning-records.md";
    }

    @Override
    public String[] getTags() {
        return new String[] {
            "Versioning",
            "Transactions",
            "Expressions",
            "Map Operations"
        };
    }

    
    private IAerospikeClient client;
    private AeroMapper mapper;
    
    @Override
    public void setup(IAerospikeClient client, AeroMapper mapper) throws Exception {
        this.mapper = mapper;
        this.client = client;
        client.truncate(null, mapper.getNamespace(TradeBase.class), mapper.getSet(TradeBase.class), null);
        client.truncate(null, mapper.getNamespace(TradeStaticData.class), mapper.getSet(TradeStaticData.class), null);
        
        ValueCreator<TradeStaticData> tradeContentCreator = new ValueCreator<>(TradeStaticData.class);
        new Generator().generate(0, 10000, TradeBase.class, trade -> {
            TradeStaticData tradeContent = tradeContentCreator.createAndPopulate(Map.of());
            tradeContent.setTradeId(trade.getId());
            tradeContent.setVersion(0);
            trade.setVersion(0);
            trade.setDataVersion(tradeContent.getVersion());
            // The current version is always -1
            Date now = new Date();
            trade.setVersions(Map.of(now.getTime(), -1));
            trade.setUpdatedDate(now);
            mapper.save(trade, tradeContent);
        }).monitor();
    }

    private Key formKey(Class<?> objectClass, long id) {
        String namespace = mapper.getNamespace(objectClass);
        String setName = mapper.getSet(objectClass);
        return new Key(namespace, setName, id);
    }
    
    private Key formKey(Class<?> objectClass, long id, int version) {
        if (version < 0) {
            return formKey(objectClass, id);
        }
        String namespace = mapper.getNamespace(objectClass);
        String setName = mapper.getSet(objectClass);
        return new Key(namespace, setName, id + ":" + version);
    }
    
    /**
     * To update the tradebase, we need to:
     * <ol>
     * <li>Create a transaction in Aerospike, since this operation needs to be atomic.</li>
     * <li>copy of the current trade base object is created (ex:  trade:12345:1), the trade base object’s version 
     * number bin is incremented by 1, the key of this version has the version number appended in the key name. 
     * If there is a versions list it is removed, since it is only present in the effective version, and not the copy.</li>
     * <li>The current trade base object’s (trade:12345) bins are updated with new information as necessary, 
     * this object’s key has no version number since it is the effective copy. The versions map is updated 
     * with the timestamp of the changes mapped to the version number of the older copy. (ex: {1672876800:1})</li>
     * <li>Commit the transaction in Aerospike</li>
     * </ol>
     * @param id - the id of the unversioned record
     * @param timestamp - the timestamp of the update (if 0, use current time)
     * @param existingTxn - the existing transaction if there is one, otherwise on will be created locally in this method
     * @param handler
     * @return - The version of the new record
     */
    private <T> int updateObjectWithVersion(Class<T> clazz, long id, long timestamp, Txn existingTxn, ChangeHandler<T> handler) {
        // 1. Create the transaction.
        return Utils.doInTransaction(client, existingTxn, txn -> {
            Policy readPolicy = client.copyReadPolicyDefault();
            readPolicy.txn = txn;
            
            WritePolicy writePolicy = client.copyWritePolicyDefault();
            writePolicy.txn = txn;
            
            Key unversionedKey = formKey(clazz, id);
            Record rec = client.get(readPolicy, unversionedKey);
            
            // 2. Copy this to a new record, excluding the version map
            int currentVersion = rec.getInt("version");
            boolean hasVersionMap = rec.getMap("versions") != null;
            
            Key versionedKey = formKey(clazz, id, currentVersion);
            Bin[] bins = rec.bins.entrySet().stream()
                .filter(entry -> !"versions".equals(entry.getKey()))
                .map(entry -> new Bin(entry.getKey(), Value.get(entry.getValue())))
                .toArray(Bin[]::new);
            writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
            client.put(writePolicy, versionedKey, bins);
            
            // 3. Update the record. Changes to any of the version map, version or id will be ignored.
            List<Operation> changes = handler.apply(rec).stream()
                    .filter(op -> op.binName != "version" && op.binName != "versions" && op.binName != "id")
                    .collect(Collectors.toList());
        
            // Update the version map
            int newVersion = currentVersion + 1;
            long tsToUse = timestamp == 0 ? new Date().getTime() : timestamp;
            changes.add(Operation.put(new Bin("version", newVersion)));
            changes.add(Operation.put(new Bin("updatedDate", tsToUse)));
            
            if (hasVersionMap) {
                // The existing version will be listed as -1 in the version map, so the code knows to load
                // the unversioned record. So we will need to change this to be the current version, and
                // insert the new one as -1
                long mapKeyOfCurrentVersion = ((Map<Long, Long>)rec.getMap("versions")).entrySet()
                    .stream()
                    .filter(e -> e.getValue() == -1)
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElseThrow(() -> new NoSuchElementException("No value mapped to -1"));
                
                changes.add(MapOperation.put(MAP_POLICY, "versions", Value.get(mapKeyOfCurrentVersion), Value.get(currentVersion)));
                changes.add(MapOperation.put(MAP_POLICY, "versions", Value.get(tsToUse), Value.get(-1)));
            }
            
            // Write the record
            writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
            client.operate(writePolicy, unversionedKey, changes.toArray(new Operation[0]));
            
            return newVersion;
        });
    }
    
    /**
     * Reads a TradeBase record as it existed at a specific point in time using versioning.
     * 
     * <p>The versioning mechanism uses a sorted map (bin name "versions") where:
     * <ul>
     *   <li>Keys are timestamps (in milliseconds) when changes occurred</li>
     *   <li>Values are version numbers indicating which version was active at that timestamp</li>
     *   <li>The special value -1 indicates the current (unversioned) record</li>
     * </ul>
     * 
     * <p><b>Versioning Operation Details:</b><br>
     * The method uses {@code MapOperation.getByKeyRelativeIndexRange} to efficiently find the correct version:
     * <pre>
     * MapOperation.getByKeyRelativeIndexRange("versions", Value.get(timestamp+1), -1, 1, MapReturnType.KEY_VALUE)
     * </pre>
     * This operation works as follows:
     * <ol>
     *   <li>Searches for the first key in the map that is <b>greater than</b> the requested timestamp (timestamp+1)</li>
     *   <li>Uses relative index -1 to go back one entry, finding the version active <b>at or before</b> the timestamp</li>
     *   <li>Returns exactly 1 entry (the version that was active at the requested time)</li>
     *   <li>Returns KEY_VALUE pairs as a list of map entries</li>
     * </ol>
     * 
     * <p><b>Example:</b><br>
     * If the versions map contains: {@code {1000: 0, 2000: 1, 3000: 2, 4000: -1}}
     * <ul>
     *   <li>Reading at timestamp 1500 returns version 0 (active from 1000-1999)</li>
     *   <li>Reading at timestamp 2500 returns version 1 (active from 2000-2999)</li>
     *   <li>Reading at timestamp 4500 returns version -1 (current version, active from 4000 onwards)</li>
     *   <li>Reading at timestamp 500 returns null (before earliest version)</li>
     * </ul>
     * 
     * <p>In a single atomic operation, this method:
     * <ul>
     *   <li>Queries the versions map to find the appropriate version number</li>
     *   <li>Reads all TradeBase fields (except "versions" itself)</li>
     * </ul>
     * 
     * <p>If the version is -1 (current), the unversioned record data is returned directly.
     * Otherwise, the method reads the historical versioned record with key format "id:version".
     * 
     * @param id the trade identifier to read
     * @param timestamp the point in time (milliseconds since epoch) to read the record state from
     * @return the TradeBase as it existed at the specified timestamp, or null if the timestamp 
     *         is before the earliest recorded version
     * @throws IllegalArgumentException if no record exists with the specified id
     */
    private TradeBase readAtTime(long id, long timestamp) {
        Key unversionedKey = formKey(TradeBase.class, id);
        
        Record rec = client.operate(null, unversionedKey, 
            MapOperation.getByKeyRelativeIndexRange("versions", Value.get(timestamp+1), -1, 1, MapReturnType.KEY_VALUE),
            Operation.get("id"),
            Operation.get("sourceSystemId"),
            Operation.get("version"),
            Operation.get("parentTradeId"),
            Operation.get("extTradeId"),
            Operation.get("contentId"),
            Operation.get("book"),
            Operation.get("counterparty"),
            Operation.get("tradeDate"),
            Operation.get("enteredDate"),
            Operation.get("updatedDate"),
            Operation.get("loadDate"),
            Operation.get("cashStlmntDate"),
            Operation.get("novationDate"),
            Operation.get("tradeVersion"),
            Operation.get("fullDocId"),
            Operation.get("dataVersion")
        );
        
        if (rec == null) {
            throw new IllegalArgumentException("No trade base with id: " + id);
        }
        // Note: The versions map operation result is available in rec.getList("versions") if needed
        List<SimpleEntry<Long, Long>> versionsList = (List<SimpleEntry<Long, Long>>) rec.getList("versions");
        if (versionsList == null || versionsList.size() == 0) {
            // This is before the earliest timestamp
            return null;
        }
        long version = versionsList.get(0).getValue();
        if (version == -1) {
            // Current version
            rec.bins.remove("versions");
            return mapper.getMappingConverter().convertToObject(TradeBase.class, unversionedKey, rec);
        }
        else {
            // Historical version, return actual version
            return mapper.read(TradeBase.class, id + ":" + version);
        }
        
        // Construct TradeBase from the record
//        TradeBase trade = new TradeBase();
//        trade.setId(rec.getLong("id"));
//        trade.setSourceSystemId(rec.getString("sourceSystemId"));
//        trade.setVersion(rec.getInt("version"));
//        trade.setParentTradeId(rec.getLong("parentTradeId"));
//        trade.setExtTradeId(rec.getString("extTradeId"));
//        trade.setContentId(rec.getLong("contentId"));
//        trade.setBook(rec.getString("book"));
//        trade.setCounterparty(rec.getString("counterparty"));
//        trade.setTradeDate((Date) rec.getValue("tradeDate"));
//        trade.setEnteredDate((Date) rec.getValue("enteredDate"));
//        trade.setUpdatedDate((Date) rec.getValue("updatedDate"));
//        trade.setLoadDate((Date) rec.getValue("loadDate"));
//        trade.setCashStlmntDate((Date) rec.getValue("cashStlmntDate"));
//        trade.setNovationDate((Date) rec.getValue("novationDate"));
//        trade.setTradeVersion(rec.getInt("tradeVersion"));
//        trade.setFullDocId(rec.getBoolean("fullDocId"));
//        trade.setDataVersion(rec.getInt("dataVersion"));
//        
//        return trade;
    }
    
    @Override
    public void run(IAerospikeClient client, AeroMapper mapper) throws Exception {
        this.mapper = mapper;
        this.client = client;

        Async.runFor(Duration.ofSeconds(5), async -> {

            async.periodic(Duration.ofMillis(200), () -> {
                long now = System.nanoTime();
                Utils.doInTransaction(client, txn -> {
                    final int tradeId = 2;
                    int newVersion = updateObjectWithVersion(TradeStaticData.class, tradeId, 0, null, rec -> {
                        return List.of(Operation.add(new Bin("mutableData", 3)));
                    });
                    updateObjectWithVersion(TradeBase.class, tradeId, 0, null, rec -> {
                        return List.of(Operation.add(new Bin("dataVersion", newVersion)));
                    });
                });
                System.out.printf("Update took %,dus\n", (System.nanoTime() - now) / 1_000);
            });

            async.periodic(Duration.ofSeconds(1), () -> {
                updateObjectWithVersion(TradeBase.class, 1, 0, null, rec -> {
                    return List.of(Operation.add(new Bin("tradeVersion", 3)));
                });
            });
        });
        long now = new Date().getTime();
        System.out.println("Version map:");
        System.out.println(client.get(null, formKey(TradeBase.class, 2), "versions"));
        System.out.printf("\nReading current version (at %d):", now);
        System.out.println(readAtTime(2, now));
        System.out.println("\nReading current version 2 seconds ago:");
        System.out.println(readAtTime(2, now - 2000));
        System.out.println("\nReading before the first version");
        System.out.println(readAtTime(2, now - 200000));
    }

}
