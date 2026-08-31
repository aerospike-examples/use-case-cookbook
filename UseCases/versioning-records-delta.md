# Delta Versioning Records (Audit Trail)
[Back to all use cases](../README.md)

Related pattern: [Versioning Records (full copy, point-in-time queries)](versioning-records.md)

[Link to working code (legacy Java client)](../source/java/src/main/java/com/aerospike/examples/recordversioning/DeltaVersioningRecords.java) | [Link to working code (Java SDK)](../source/java-sdk/src/main/java/com/aerospike/examples/recordversioning/DeltaVersioningRecords.java)

## Use Case

When regulatory or operational requirements call for an **audit trail** rather than point-in-time record retrieval, storing a full copy of every version is wasteful — especially when changes are small and frequent. The delta versioning pattern addresses this by:

- Storing **only what changed** at each version, plus audit metadata (who, when, description)
- Detecting bin-level changes automatically server-side, distinguishing real changes from no-op writes
- Optionally storing **new bin values** in each delta so records can be reconstructed by replay

Unlike the [full-copy pattern](versioning-records.md), delta versioning **cannot** efficiently retrieve a record as it existed at an arbitrary past timestamp without replaying deltas. It is optimised for audit and compliance reporting rather than temporal queries. This use case versions **`TradeBase` only** — it does not version associated `TradeStaticData`.

### When to Use Each Pattern

| Requirement | Full Version Copy | Delta Audit Trail |
|-------------|-------------------|-------------------|
| Point-in-time record retrieval | ✅ | ❌ (requires replay) |
| Audit trail (who/what/when) | Partial | ✅ |
| Storage efficiency for small changes | ❌ | ✅ |
| Record reconstruction | ✅ (direct read) | ✅ (with `recordContents`) |
| Associated large content versioning | ✅ (`TradeStaticData`) | Not applicable |

## Data Model

Delta versioning uses the same `TradeBase` model and primary key strategy as the full-copy pattern for the **live record**, but versioned keys store **audit deltas** rather than full snapshots.

### TradeBase

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
    private int tradeVersion;
    private boolean recordComplete;
    private int dataVersion;
    @GenExclude
    private Map<Long, Integer> versions = new HashMap<>();  // Point-in-time version map
}
```

### Live Record (key: `id`)

Always holds the **current effective** `TradeBase` state, plus:

- `version` — current version number
- `versions` — key-ordered map for point-in-time version lookup (same semantics as the full-copy pattern)

Example: key `2` holds the latest trade data.

### Delta Records (key: `id:0`, `id:1`, …)

Each delta record is an **immutable audit entry** — not a full record copy. All versions, including the initial insert, follow this shape:

```
description:  "Update counterparty and book"
user:         "alice"
changeTs:     1737668121000
deltaVer:     2
changes:      [
                { binName: "counterparty", status: "Changed", newValue: "CP-1001" },
                { binName: "book",         status: "Changed", newValue: "XY" }
              ]
```

- **Version 0** (`id:0`) — created on initial insert; every written bin is listed with status `Inserted`
- **Version 1+** (`id:1`, `id:2`, …) — only bins that actually changed (status other than `Same`)

When the `recordContents` parameter is enabled (legacy Java client), each change entry includes `newValue` (except for `Removed`), allowing [`reconstructAtVersion()`](../source/java/src/main/java/com/aerospike/examples/recordversioning/DeltaVersioningRecords.java) to replay history from version 0 onward. The Java SDK port ([`reconstructAtVersion()`](../source/java-sdk/src/main/java/com/aerospike/examples/recordversioning/DeltaVersioningRecords.java)) always records `newValue` - it doesn't have a `recordContents` toggle.

### The Versions Map

The live record carries the same key-ordered `versions` map used by the full-copy pattern:

```
versions: {
    1737654321000: 0,     // Version 0 became active at this timestamp
    1737658921000: 1,
    1737663521000: 2,
    1737668121000: -1     // Current effective version (special marker)
}
```

The `-1` value marks the timestamp of the **current** effective version. On each update, a server-side map expression closes the prior `-1` entry to the previous version number and appends a new `{changeTs: -1}` entry — all within the same atomic `operate` on the live record.

## Bin Change Detection

Changes are detected server-side using Aerospike expressions in a **single `operate()`** call on the live record:

1. **Capture version** — Current `version` is copied to temporary bin `_cv` (or `-1` if the record does not yet exist)
2. **Snapshot** — Each bin targeted by caller operations is copied to `_t0`, `_t1`, … before writes are applied
3. **Apply** — Caller's `List<Operation>` is executed (puts, adds, etc.)
4. **Compare** — An expression writes the result to `_a0`, `_a1`, …:

| Status | Meaning |
|--------|---------|
| `Inserted` | Bin did not exist before; now has a value |
| `Changed` | Bin existed and its value changed |
| `TypeChanged` | Bin existed but its particle type changed |
| `Removed` | Bin had a value before; now removed (null) |
| `Same` | Write was applied but value is unchanged — **excluded from the delta** |

5. **Clean up** — Temporary bins `_t*`, `_a*`, and `_cv` are cleared
6. **Version** — Increment `version`, update `versions` map via `MapExp`, set `updatedDate`
7. **Delta write** — A separate `put` creates the delta record at `id:newVersion`

#### Example: updating two fields in one version

Suppose trade `2` is at version `1` with `counterparty="CP-2001"` and `book="AB"`, and the caller invokes:

```java
updateTradeBaseWithDelta(2, 0, "Update counterparty and book", "alice", null,
    List.of(
        Operation.put(new Bin("counterparty", "CP-1001")),
        Operation.put(new Bin("book", "XY"))));
```

Within the single `operate` on key `2`, the server runs:

| Step | What happens |
|------|----------------|
| Capture | `_cv` ← `1` |
| Snapshot | `_t0` ← `"CP-2001"` (counterparty), `_t1` ← `"AB"` (book) |
| Apply | `counterparty` ← `"CP-1001"`, `book` ← `"XY"` |
| Compare | `_a0` ← `"Changed"`, `_a1` ← `"Changed"` |
| Version | `version` ← `2`; `versions` map updated |
| Clean up | `_cv`, `_t0`, `_t1`, `_a0`, `_a1` cleared |

The delta record written to key `2:2` contains:

```
description: "Update counterparty and book"
user:        "alice"
changeTs:    <timestamp>
deltaVer:    2
changes:     [
               { binName: "counterparty", status: "Changed" },
               { binName: "book",         status: "Changed" }
             ]
```

If `recordContents` is enabled, each entry also includes `newValue` (`"CP-1001"` and `"XY"` respectively).

If `book` had already been `"XY"`, the comparison would classify it as `Same` and it would be **omitted** from `changes` — only `counterparty` would appear on the delta record.

**Temporary bins** (all ≤ 15 characters):

| Bin | Purpose |
|-----|---------|
| `_cv` | Version captured before user operations |
| `_t0`, `_t1`, … | Pre-write snapshots of targeted bins |
| `_a0`, `_a1`, … | Comparison status per targeted bin |

> **Record size caveat:** Each targeted bin is copied into a temporary bin (`_tN`) for the duration
> of the `operate`. While those temps exist, the live record holds **both** the permanent bins and
> their snapshots. If the combined size exceeds the namespace's `max-record-size` (or 8 MB,
> whichever is lower), the operation fails with `RECORD_TOO_BIG`. For example, updating a 5 MB blob
> bin temporarily doubles that bin on the record (~10 MB) and will fail on most clusters. **Use this
> pattern only on records whose steady-state size is well under half the maximum record size** —
> large or blob-heavy fields are better versioned with the [full-copy pattern](versioning-records.md)
> or kept in a separate set.

**Note:** Aerospike cannot compare unordered maps. If a map bin is being changed, `MapOperation.setMapPolicy(KEY_ORDERED)` is applied first.

Particle types for expression comparisons are resolved from a hard-coded map of `TradeBase` field names (dates stored as `INTEGER`). Operations on protected bins `version` and `versions` in caller input are ignored.

## Creating a Delta Version

The `updateTradeBaseWithDelta()` method accepts a `List<Operation>` and performs the update atomically within a transaction:

```java
public int updateTradeBaseWithDelta(long id, long timestamp, String description, String user,
        Txn existingTxn, List<Operation> userOps)
```

**Flow:**

1. Filter protected bins from `userOps`
2. One atomic `operate` on key `id`: snapshot → apply → compare → increment version → update `versions` map
3. Parse comparison results; build `changes` list (excluding `Same`)
4. `put` delta record at `id:newVersion` with `CREATE_ONLY`

Example — single-bin update:

```java
updateTradeBaseWithDelta(tradeId, 0, "Update counterparty", "alice", null,
    List.of(Operation.put(new Bin("counterparty", "CP-1001"))));
```

Example — multi-bin update in one version:

```java
updateTradeBaseWithDelta(tradeId, 0, "Update counterparty and book", "alice", null,
    List.of(
        Operation.put(new Bin("counterparty", "CP-1001")),
        Operation.put(new Bin("book", "XY"))));
```

### Initial Insert (Setup)

Setup truncates the set and creates `numRecords` trades (default `100`, override with
`--param.numrecords=N`). It does not call `mapper.save`. Each generated trade is converted
with `convertToMap()` into put operations and routed through `updateTradeBaseWithDelta`,
so the initial insert uses the same code path as updates. Delta `id:0` lists every bin as
`Inserted`.

### Versions Map Update (MapExp)

The `versions` map is maintained server-side in the same `operate` as the data update:

- **Initial insert** (`_cv == -1`): seed `{changeTs: -1}`
- **Subsequent updates**: find the key whose value is `-1`, set it to `_cv` (the prior version), append `{changeTs: -1}`

This avoids a second round-trip to the live record.

## Record Reconstruction

When `recordContents=true` (use case parameter `--param.recordcontents=true` or constructor flag), each delta stores `newValue` for changed bins. A record at any version can be reconstructed by replaying deltas from version 0:

```java
Record atVersion2 = reconstructAtVersion(tradeId, 2);
// Applies changes from id:0, id:1, id:2 in order
```

Replay rules:

- `Inserted`, `Changed`, `TypeChanged` → set bin to `newValue`
- `Removed` → remove bin from reconstructed record

Without `recordContents`, deltas contain only bin names and status strings — sufficient for audit reporting but not reconstruction.

## Running the Demo

```bash
cd source && mvn compile exec:java \
  -Dexec.mainClass="com.aerospike.examples.UseCaseCookbookRunner" \
  -Dexec.args="-uc 'Delta Versioning Records' -h localhost:3100"
```

With record reconstruction:

```bash
-Dexec.args="-uc 'Delta Versioning Records' -h localhost:3100 --param.recordcontents=true"
```

The demo performs three sequential updates on trade id 2 (including a two-bin change), prints the audit trail, and displays the `versions` map:

```java
updateTradeBaseWithDelta(tradeId, 0, "Increment trade version", "batch-user", null,
    List.of(Operation.add(new Bin("tradeVersion", 1))));

updateTradeBaseWithDelta(tradeId, 0, "Update counterparty and book", "alice", null,
    List.of(
        Operation.put(new Bin("counterparty", "CP-1001")),
        Operation.put(new Bin("book", "XY"))));

updateTradeBaseWithDelta(tradeId, 0, "Mark record complete", "bob", null,
    List.of(Operation.put(new Bin("recordComplete", true))));
```

## AQL Examples

### Live Record

```sql
aql> select version, versions, counterparty, book from test.tradebase where PK = 2
+---------+--------------------------------------------------------------------+--------------+------+
| version | versions                                                           | counterparty | book |
+---------+--------------------------------------------------------------------+--------------+------+
| 3       | MAP('{1785743955565:0, 1785743960437:1, 1785743960457:2,           | "CP-1001"  | "XY" |
|         |      1785743960459:-1}')                                           |              |      |
+---------+--------------------------------------------------------------------+--------------+------+
```

### Initial Insert Delta (version 0)

```sql
aql> select deltaVer, description, user, changes from test.tradebase where PK = "2:0"
+----------+-----------------+--------+---------------------------+
| deltaVer | description     | user   | changes                   |
+----------+-----------------+--------+---------------------------+
| 0        | "Initial insert"| "setup"| LIST('[{"binName":"book", |
|          |                 |        | "status":"Inserted"}, ... |
+----------+-----------------+--------+---------------------------+
```

### Change Delta (multi-bin update)

```sql
aql> select * from test.tradebase where PK = "2:2"
+-------------+--------------------------------------------------+
| changeTs    | 1785743960457                                    |
+-------------+--------------------------------------------------+
| changes     | LIST('[{"binName":"book","status":"Changed"},    |
|             | {"binName":"counterparty","status":"Changed"}]') |
+-------------+--------------------------------------------------+
| deltaVer    | 2                                                |
+-------------+--------------------------------------------------+
| description | "Update counterparty and book"                   |
+-------------+--------------------------------------------------+
| user        | "alice"                                          |
+-------------+--------------------------------------------------+
```

## Performance Considerations

### Storage

- **Live record** (`id`): Standard `TradeBase` size + `versions` map
- **Delta records**: Metadata plus changed bins only — typically much smaller than full copies
- **Version 0**: Lists all bins as `Inserted` (metadata only unless `recordContents` is enabled)

For a record with 100 small changes (1–2 bins each):

- **Full copy**: 100 × ~4.5 KB ≈ 450 KB
- **Delta**: 100 × ~0.5 KB ≈ 50 KB (plus live record)

Delta storage is significantly more efficient when changes are incremental.

### Record Size During Updates

The snapshot-and-compare approach temporarily duplicates each targeted bin on the live record. Peak
size during an update is roughly the permanent record plus the sum of targeted bin sizes (plus small
overhead for `_cv`, `_aN`, etc.). Plan for records to remain below **half** of `max-record-size`
(or 8 MB, whichever is lower) so that updating even the largest bin cannot exceed the limit. See
the [record size caveat](#bin-change-detection) in Bin Change Detection.

### Database Operations per Update

| Step | Key | Operation |
|------|-----|-----------|
| Live record update | `id` | One atomic `operate` (data + comparison + version + versions map) |
| Delta write | `id:N` | One `put` (`CREATE_ONLY`) |

### Query Patterns

- **Audit trail**: O(V) reads — one read per delta from `id:0` through `id:currentVersion`
- **Reconstruction at version N**: O(N) reads + in-memory replay (requires `recordContents`)
- **Current record**: O(1) — direct key lookup on `id`

## Summary

The delta versioning pattern provides:

✅ **Efficient audit trail** — Only changed bins are stored per version  
✅ **Automatic change detection** — Server-side expressions exclude no-op writes  
✅ **Rich metadata** — Who, when, and why for every change  
✅ **Optional reconstruction** — Replay deltas when `recordContents` is enabled  
✅ **Single live-record write** — Version and versions map updated in one `operate`  

Choose this pattern when you need to know **who changed what**. Choose [full version copy](versioning-records.md) when you need **point-in-time record retrieval** including associated `TradeStaticData`.

## Source Code

- DeltaVersioningRecords.java: [legacy](../source/java/src/main/java/com/aerospike/examples/recordversioning/DeltaVersioningRecords.java) | [Java SDK](../source/java-sdk/src/main/java/com/aerospike/examples/recordversioning/DeltaVersioningRecords.java)
- TradeBase.java: [legacy](../source/java/src/main/java/com/aerospike/examples/recordversioning/model/TradeBase.java) | [Java SDK](../source/java-sdk/src/main/java/com/aerospike/examples/recordversioning/model/TradeBase.java)
