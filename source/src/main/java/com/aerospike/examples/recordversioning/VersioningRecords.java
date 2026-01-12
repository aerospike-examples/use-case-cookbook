package com.aerospike.examples.recordversioning;

import java.time.Duration;
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
        return "Versioning records use case";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/versioningRecords.md";    }

    
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
    
    @Override
    public void run(IAerospikeClient client, AeroMapper mapper) throws Exception {
        this.mapper = mapper;
        this.client = client;

        Async.runFor(Duration.ofSeconds(5), async -> {

            async.periodic(Duration.ofSeconds(2), () -> {
                Utils.doInTransaction(client, txn -> {
                    final int tradeId = 2;
                    int newVersion = updateObjectWithVersion(TradeStaticData.class, tradeId, 0, null, rec -> {
                        return List.of(Operation.add(new Bin("mutableData", 3)));
                    });
                    updateObjectWithVersion(TradeBase.class, tradeId, 0, null, rec -> {
                        return List.of(Operation.add(new Bin("dataVersion", newVersion)));
                    });
                });
            });

            async.periodic(Duration.ofSeconds(1), () -> {
                updateObjectWithVersion(TradeBase.class, 1, 0, null, rec -> {
                    return List.of(Operation.add(new Bin("tradeVersion", 3)));
                });
            });
        });
    }

}
