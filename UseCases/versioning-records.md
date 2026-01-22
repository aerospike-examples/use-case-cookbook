# Versioning Records
[Back to all use cases](../README.md)

[Link to working code](../source/src/main/java/com/aerospike/examples/recordversioning/VersioningRecords.java)

## Use Case

Many applications require the ability to maintain historical versions of records, allowing for point-in-time quering. For exmpale, financial trading systems need audit trails showing how trade details evolved over time, and data analysts need to perform temporal queries to understand how business objects changed.

This use case demonstrates a pattern for maintaining full version history of records with efficient point-in-time query capabilities. This pattern provides:

- Complete version history with two levels of objects maintained
    - `TradeBase` objects are details on a particular trade. These are fairly lightweight and can change often
    - `TradeStaticData` objects are associate with a `TradeBase` and are large, typically slower changing data.
- Point-in-time queries to retrieve records as they existed at any timestamp
- Atomic version creation using transactions
- Minimal overhead for reading current (non-historical) data

## Real-World Applications

This versioning pattern is applicable to:

- **Financial Trading Systems** - Track how trade details (prices, quantities, counterparties) change over time for audit and compliance
- **Legal/Contract Management** - Maintain complete history of contract terms and amendments
- **Healthcare Records** - Store patient data changes while maintaining HIPAA-compliant audit trails
- **Regulatory Compliance** - Prove what data existed at specific times for regulatory inquiries
- **Data Quality Analysis** - Understand how data corrections and updates evolved
- **Temporal Data Analysis** - Perform historical analysis and "what-if" scenarios

## Data Model

The versioning pattern uses two complementary data structures:

### TradeBase - The Versioned Record

```java
@Data
@AerospikeRecord(namespace = "test", set = "tradebase")
@GenMagic
public class TradeBase {
    @AerospikeKey
    @GenExpression("$Key")
    private long id;
    private String sourceSystemId;
    private int version;              // Current version number (0, 1, 2, ...)
    private long parentTradeId;
    private String extTradeId;
    private long contentId;
    private String book;
    private String counterparty;
    private Date tradeDate;
    private Date enteredDate;
    private Date updatedDate;
    private Date loadDate;
    private Date cashStlmntDate;
    private Date novationDate;
    private int tradeVersion;
    private boolean fullDocId;
    private int dataVersion;
    @GenExclude
    private Map<Long, Integer> versions = new HashMap<>();  // The version history map
}
```

### TradeStaticData - Associated Content

```java
@Data
@AerospikeRecord(namespace = "test", set = "tradecontent")
public class TradeStaticData {
    @AerospikeKey
    private long tradeId;
    private int version;
    @GenString(type = StringType.WORDS, minLength = 500, maxLength = 2000)
    private String data;          // Large static content
    private int mutableData;      // Content that can change between versions
}
```

## Key Concepts

### Version Numbering

- Versions are numbered sequentially starting from 0: `0, 1, 2, 3, ...`
- Each update creates a new version with an incremented version number
- The version number is stored in the `version` bin

### Primary Key Strategy

The system uses two different key formats:

1. **Current Version (Unversioned Key)**: `id`
   - Example: `12345`
   - Always contains the most recent data
   - Includes the versions map for point-in-time queries

2. **Historical Versions (Versioned Key)**: `id:version`
   - Example: `12345:0`, `12345:1`, `12345:2`
   - Immutable snapshots of past states
   - Does NOT include the versions map (only in current record)

### The Versions Map

The versions map is a **key-ordered map** stored in the current (unversioned) record:

```
versions: {
    1737654321000: 0,     // At timestamp X, version 0 was active
    1737658921000: 1,     // At timestamp Y, version 1 became active
    1737663521000: 2,     // At timestamp Z, version 2 became active
    1737668121000: -1     // At current time, "current" version is active
}
```

**Key features:**
- **Keys**: Timestamps in milliseconds (when each version became active)
- **Values**: Version numbers (0, 1, 2, ...) or the special value `-1`
- **Special Value -1**: Indicates the current (unversioned) record
- **Ordered**: Map is sorted by timestamp for efficient range queries
- **Only in Current Record**: Historical versioned records don't have this map

### Copy-on-Write Pattern

When updating a versioned record:

1. The current record (e.g., key `12345`) is **copied** to a versioned record (e.g., key `12345:2`)
2. The current record is then **updated** with new data and version incremented
3. The versions map is **updated** to reflect the new version timeline
4. All operations happen **atomically** within a transaction

This ensures:
- No data loss during updates
- Point-in-time consistency
- Efficient storage (only changed data creates new versions)

## Implementation Details

### Creating a New Version

The `updateObjectWithVersion()` method demonstrates the atomic version creation process:

```java
private <T> int updateObjectWithVersion(Class<T> clazz, long id, 
        long timestamp, Txn existingTxn, ChangeHandler<T> handler) {
    return Utils.doInTransaction(client, existingTxn, txn -> {
        Policy readPolicy = client.copyReadPolicyDefault();
        readPolicy.txn = txn;
        
        WritePolicy writePolicy = client.copyWritePolicyDefault();
        writePolicy.txn = txn;
        
        Key unversionedKey = formKey(clazz, id);
        Record rec = client.get(readPolicy, unversionedKey);
        
        // 1. Copy current record to versioned record
        int currentVersion = rec.getInt("version");
        Key versionedKey = formKey(clazz, id, currentVersion);
        Bin[] bins = rec.bins.entrySet().stream()
            .filter(entry -> !"versions".equals(entry.getKey()))
            .map(entry -> new Bin(entry.getKey(), Value.get(entry.getValue())))
            .toArray(Bin[]::new);
        writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        client.put(writePolicy, versionedKey, bins);
        
        // 2. Apply changes to current record
        List<Operation> changes = handler.apply(rec);
        int newVersion = currentVersion + 1;
        long tsToUse = timestamp == 0 ? new Date().getTime() : timestamp;
        changes.add(Operation.put(new Bin("version", newVersion)));
        changes.add(Operation.put(new Bin("updatedDate", tsToUse)));
        
        // 3. Update the versions map
        if (hasVersionMap) {
            // Find the current version entry (value = -1) and update it
            long mapKeyOfCurrentVersion = ((Map<Long, Long>)rec.getMap("versions"))
                .entrySet().stream()
                .filter(e -> e.getValue() == -1)
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow();
            
            // Replace -1 with actual version number
            changes.add(MapOperation.put(MAP_POLICY, "versions", 
                Value.get(mapKeyOfCurrentVersion), Value.get(currentVersion)));
            // Add new entry with -1 for current version
            changes.add(MapOperation.put(MAP_POLICY, "versions", 
                Value.get(tsToUse), Value.get(-1)));
        }
        
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
        client.operate(writePolicy, unversionedKey, changes.toArray(new Operation[0]));
        
        return newVersion;
    });
}
```

**Key steps:**
1. **Read current record** within transaction
2. **Copy to versioned key** (e.g., `12345:2`) excluding the versions map
3. **Apply business changes** to current record
4. **Update version number** and timestamp
5. **Update versions map**: Change old `-1` entry to actual version number, add new `-1` entry for current
6. **Commit transaction** atomically

### Point-in-Time Queries

The `readAtTime()` method retrieves a record as it existed at a specific timestamp using an efficient map operation:

```java
private TradeBase readAtTime(long id, long timestamp) {
    Key unversionedKey = formKey(TradeBase.class, id);
    
    // Perform single atomic operation to get version and all fields
    Record rec = client.operate(null, unversionedKey, 
        MapOperation.getByKeyRelativeIndexRange("versions", 
            Value.get(timestamp+1), -1, 1, MapReturnType.KEY_VALUE),
        Operation.get("id"),
        Operation.get("sourceSystemId"),
        // ... all other fields ...
    );
    
    if (rec == null) {
        throw new IllegalArgumentException("No trade base with id: " + id);
    }
    
    List<SimpleEntry<Long, Long>> versionsList = 
        (List<SimpleEntry<Long, Long>>) rec.getList("versions");
    
    if (versionsList == null || versionsList.size() == 0) {
        return null;  // Timestamp before earliest version
    }
    
    long version = versionsList.get(0).getValue();
    if (version == -1) {
        // Current version - use data from the record we just read
        rec.bins.remove("versions");
        return mapper.getMappingConverter()
            .convertToObject(TradeBase.class, unversionedKey, rec);
    } else {
        // Historical version - read the versioned record
        return mapper.read(TradeBase.class, id + ":" + version);
    }
}
```

#### Understanding the Map Operation

The critical operation is:

```java
MapOperation.getByKeyRelativeIndexRange("versions", 
    Value.get(timestamp+1), -1, 1, MapReturnType.KEY_VALUE)
```

**How it works:**

Given a versions map like:
```
{1000: 0, 2000: 1, 3000: 2, 4000: -1}
```

When querying at timestamp `2500`:

1. **Search for key > timestamp**: Look for first key > `2501` (timestamp+1)
   - Finds `3000` at index 2
2. **Use relative index -1**: Go back one entry from index 2
   - Returns entry at index 1: `{2000: 1}`
3. **Return 1 entry**: Only return this single key-value pair
4. **Result**: Version `1` was active at timestamp `2500`

**Example queries:**

| Timestamp | Operation Result | Explanation |
|-----------|-----------------|-------------|
| `1500` | Version `0` | Active from 1000-1999 |
| `2500` | Version `1` | Active from 2000-2999 |
| `4500` | Version `-1` (current) | Active from 4000 onwards |
| `500` | `null` | Before earliest version |

This algorithm is **O(log N)** on the number of versions due to the key-ordered map structure, making it very efficient even with thousands of versions.

## Running the Demo

The demo generates 10,000 trade records, then continuously updates a subset of them to create version history:

```java
@Override
public void run(IAerospikeClient client, AeroMapper mapper) throws Exception {
    Async.runFor(Duration.ofSeconds(5), async -> {
        // Periodically update trade 2 to create version history
        async.periodic(Duration.ofMillis(200), () -> {
            Utils.doInTransaction(client, txn -> {
                final int tradeId = 2;
                // Update both TradeStaticData and TradeBase
                int newVersion = updateObjectWithVersion(TradeStaticData.class, 
                    tradeId, 0, null, rec -> {
                        return List.of(Operation.add(new Bin("mutableData", 3)));
                    });
                updateObjectWithVersion(TradeBase.class, tradeId, 0, null, rec -> {
                    return List.of(Operation.put(new Bin("dataVersion", newVersion)));
                });
            });
        });

        // Update another trade to show multiple records with versions
        async.periodic(Duration.ofSeconds(1), () -> {
            updateObjectWithVersion(TradeBase.class, 1, 0, null, rec -> {
                return List.of(Operation.add(new Bin("tradeVersion", 3)));
            });
        });
    });
    
    // Demonstrate point-in-time queries
    long now = new Date().getTime();
    System.out.println("Version map:");
    System.out.println(client.get(null, formKey(TradeBase.class, 2), "versions"));
    
    System.out.printf("\nReading current version (at %d):", now);
    System.out.println(readAtTime(2, now));
    
    System.out.println("\nReading version 2 seconds ago:");
    System.out.println(readAtTime(2, now - 2000));
    
    System.out.println("\nReading before the first version:");
    System.out.println(readAtTime(2, now - 200000));
}
```

## AQL Examples

### Current Record Structure

The current (unversioned) record contains all data plus the versions map:

```sql
aql> select * from test.tradebase where PK = 2
+------+------------------+--------+---------+---------------+---------------+
| book | cashStlmntDate   | contentId | counterparty | dataVersion  | enteredDate  |
+------+------------------+--------+---------+---------------+---------------+
| "XY" | 1674852736142    | 5281    | "ACME Corp"  | 3            | 1737654321000 |
+------+------------------+--------+---------+---------------+---------------+

+---------------+---------------+-------------+---------------+-----------------+
| extTradeId    | fullDocId     | id          | loadDate      | novationDate    |
+---------------+---------------+-------------+---------------+-----------------+
| "EXT-12345"   | true          | 2           | 1737654321100 | 0               |
+---------------+---------------+-------------+---------------+-----------------+

+----------------+------------------+--------------+---------------+---------+
| parentTradeId  | sourceSystemId   | tradeDate    | tradeVersion  | updatedDate |
+----------------+------------------+--------------+---------------+---------+
| 0              | "SYS-001"        | 1737650000000 | 12           | 1737668121000 |
+----------------+------------------+--------------+---------------+---------+

+---------+--------------------------------------------------------------------+
| version | versions                                                            |
+---------+--------------------------------------------------------------------+
| 3       | MAP('{1737654321000:0, 1737658921000:1, 1737663521000:2,           |
|         |      1737668121000:-1}')                                           |
+---------+--------------------------------------------------------------------+
1 row in set (0.003 secs)
```

### Historical Versioned Record

Historical versions use composite keys (`id:version`) and do NOT include the versions map:

```sql
aql> select * from test.tradebase where PK = "2:1"
+------+------------------+--------+---------+---------------+---------------+
| book | cashStlmntDate   | contentId | counterparty | dataVersion  | enteredDate  |
+------+------------------+--------+---------+---------------+---------------+
| "XY" | 1674852736142    | 5281    | "ACME Corp"  | 1            | 1737654321000 |
+------+------------------+--------+---------+---------------+---------------+

+---------------+---------------+-------------+---------------+-----------------+
| extTradeId    | fullDocId     | id          | loadDate      | novationDate    |
+---------------+---------------+-------------+---------------+-----------------+
| "EXT-12345"   | true          | 2           | 1737654321100 | 0               |
+---------------+---------------+-------------+---------------+-----------------+

+----------------+------------------+--------------+---------------+---------+
| parentTradeId  | sourceSystemId   | tradeDate    | tradeVersion  | updatedDate |
+----------------+------------------+--------------+---------------+---------+
| 0              | "SYS-001"        | 1737650000000 | 6            | 1737658921000 |
+----------------+------------------+--------------+---------------+---------+

+---------+
| version |
+---------+
| 1       |
+---------+
1 row in set (0.002 secs)

OK
```

### Examining the Versions Map

The versions map shows the complete timeline:

```sql
aql> select versions from test.tradebase where PK = 2
+--------------------------------------------------------------------+
| versions                                                            |
+--------------------------------------------------------------------+
| MAP('{1737654321000:0, 1737658921000:1, 1737663521000:2,           |
|      1737668121000:-1}')                                           |
+--------------------------------------------------------------------+

Interpretation:
- At timestamp 1737654321000: version 0 became active
- At timestamp 1737658921000: version 1 became active  
- At timestamp 1737663521000: version 2 became active
- At timestamp 1737668121000: current version (indicated by -1) became active
```

### Querying All Versions of a Record

To see all versions of a particular record:

```sql
aql> select version, updatedDate, tradeVersion from test.tradebase where PK = 2
+---------+---------------+--------------+
| version | updatedDate   | tradeVersion |
+---------+---------------+--------------+
| 3       | 1737668121000 | 12           |
+---------+---------------+--------------+

aql> select version, updatedDate, tradeVersion from test.tradebase where PK = "2:0"
+---------+---------------+--------------+
| version | updatedDate   | tradeVersion |
+---------+---------------+--------------+
| 0       | 1737654321000 | 0            |
+---------+---------------+--------------+

aql> select version, updatedDate, tradeVersion from test.tradebase where PK = "2:1"
+---------+---------------+--------------+
| version | updatedDate   | tradeVersion |
+---------+---------------+--------------+
| 1       | 1737658921000 | 6            |
+---------+---------------+--------------+

aql> select version, updatedDate, tradeVersion from test.tradebase where PK = "2:2"
+---------+---------------+--------------+
| version | updatedDate   | tradeVersion |
+---------+---------------+--------------+
| 2       | 1737663521000 | 9            |
+---------+---------------+--------------+
```

## Performance Considerations

### Storage Overhead

- **Current Record**: Standard size + versions map (typically < 1KB for hundreds of versions)
- **Historical Records**: One full record per version (no versions map)
- **Trade-off**: Storage space vs. complete audit trail

For a record with 100 versions over its lifetime:
- Current record: ~5KB
- Historical records: 100 × ~4.5KB = ~450KB
- Total: ~455KB for complete history

### Query Performance

- **Read Current Version**: O(1) - Direct key lookup
- **Point-in-Time Query**: O(log N) where N = number of versions
  - Map lookup is O(log N) due to key-ordered structure
  - If version is current (-1): data already retrieved in same operation
  - If version is historical: additional O(1) lookup by versioned key
- **List All Versions**: O(V) where V = number of versions

### Update Performance

- **Creating New Version**: Requires transaction with 2-3 database operations:
  1. Read current record
  2. Write versioned copy
  3. Update current record (with versions map update)
- **Transaction overhead**: Uses Aerospike's MRT (Multi-Record Transactions)
- **Concurrency**: Transactions handle concurrent updates with automatic retry

### Optimization Strategies

1. **Limit Version Retention**: Implement time-based or count-based purging of old versions
2. **Compress Large Fields**: Use compression for large text fields before versioning
3. **Separate Static Data**: Use separate sets for data that rarely changes (like `TradeStaticData`)
4. **Batch Operations**: When querying multiple point-in-time records, use batch reads
5. **Version Compaction**: Periodically compact versions map by removing very old entries

## Advanced Patterns

### Version Retention Policies

Implement automatic cleanup of old versions:

```java
// Keep only last N versions
private void purgeOldVersions(long id, int keepCount) {
    // Read versions map
    // Identify versions to delete (all except newest N)
    // Delete old versioned records
    // Update versions map to remove old entries
}

// Keep versions within time window
private void purgeVersionsOlderThan(long id, long cutoffTimestamp) {
    // Similar to above but filter by timestamp
}
```

### Diffing Versions

Compare two versions to see what changed:

```java
public Map<String, Object> diffVersions(long id, int version1, int version2) {
    TradeBase v1 = mapper.read(TradeBase.class, id + ":" + version1);
    TradeBase v2 = mapper.read(TradeBase.class, id + ":" + version2);
    // Compare fields and return differences
}
```

### Bulk Point-in-Time Queries

Query multiple records at the same timestamp efficiently:

```java
public List<TradeBase> readMultipleAtTime(List<Long> ids, long timestamp) {
    // Build batch operations for all IDs
    // Execute in parallel
    // Return list of records as they existed at timestamp
}
```

## Related Patterns

- **Soft Deletes**: Simpler pattern for "deleted" flag without full history
- **Event Sourcing**: Store events that led to state changes rather than states themselves
- **Temporal Tables**: SQL databases' built-in temporal support (similar concept, different implementation)
- **Append-Only Logs**: Kafka-style immutable logs (different access patterns)

## Summary

The versioning records pattern provides:

✅ **Complete Audit Trail** - Every change is preserved with timestamp
✅ **Point-in-Time Queries** - Efficiently retrieve historical states
✅ **ACID Guarantees** - Transactions ensure consistency
✅ **Scalable Storage** - Copy-on-write minimizes storage overhead
✅ **High Performance** - O(log N) temporal queries, O(1) current reads
✅ **Flexible Retention** - Easy to implement custom retention policies

This pattern is ideal for applications requiring regulatory compliance, audit trails, temporal analysis, or the ability to "time travel" through data history.

## Source Code

Full implementation available at:
- [VersioningRecords.java](../source/src/main/java/com/aerospike/examples/recordversioning/VersioningRecords.java)
- [TradeBase.java](../source/src/main/java/com/aerospike/examples/recordversioning/model/TradeBase.java)
- [TradeStaticData.java](../source/src/main/java/com/aerospike/examples/recordversioning/model/TradeStaticData.java)
