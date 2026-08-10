# Hot Key — Write (HotKeyReducer)
[Back to all use cases](../README.md)

Related patterns: [Read (Replica Spread)](hot-key-read-replica-spread.md) · [Write (Shard + Merge)](hot-key-write-shard-merge.md)

[Link to working code](../source/src/main/java/com/aerospike/examples/hotkeys/ReducerHotKeyUseCase.java)

## The Problem

Some use cases **cannot** split a counter across shards — you need a single authoritative key, or changing the data model is too costly. When many threads in the same application instance call `operate` on that one key simultaneously, each call is a separate server round trip competing for the same record queue.

This is the write hot key problem without a schema change option: same rw-hash contention, same `KEY_BUSY` risk, same latency inflation — but sharding is off the table.

## How the Technique Works

**In-process batching** (here via [HotKeyReducer](../source/src/main/java/com/aerospike/examples/hotkeys/helper/HotKeyReducer.java)) reduces server-side contention by coalescing many client-side operations into **one Aerospike `operate` call per batch** for a hot key.

The reducer sits between your application threads and the Aerospike client:

```
  Thread 1 ──operate(add +1)──┐
  Thread 2 ──operate(add +1)──┤
  Thread 3 ──operate(add +1)──┼──► HotKeyReducer ──► single operate(add +3) ──► Aerospike
  Thread 4 ──operate(add +1)──┤         ▲
  Thread 5 ──operate(add +1)──┘    detects hot key,
                                   waits briefly,
                                   batches ops
```

### Step-by-step

1. **Monitor access rate per key.** The reducer tracks how many times each key is accessed within a sliding window. When accesses in the same millisecond exceed `hotThreshold` (default 3), the key is marked **hot** for `hotDurationMs` (default 500ms).

2. **Delay and collect.** The first operation for a hot key starts a short timer (`reducerDelayMs`, default 1ms). Subsequent operations for the same key during that window are **queued in memory** rather than sent to Aerospike immediately.

3. **Flush one operation.** When the timer fires, the reducer combines all queued operations into a **single** `client.operate` call. For multiple `Operation.add` on the same bin, Aerospike applies them atomically in one transaction — one rw-hash entry instead of N.

4. **Return results to callers.** Each submitting thread receives its result (or a shared result, depending on operation type) via a `CompletableFuture` completed when the batch finishes.

### Why this removes the hot key

The hot key problem at the server is **too many concurrent transactions on one record**. The reducer attacks the source: it converts N concurrent client requests into 1 server request. Fewer pending transactions means fewer `KEY_BUSY` errors and lower queue latency — without changing the Aerospike key or data model.

Note that this technique only works if many updates to the same record occur within one application instance. If the accesses are spread over many different instances the rate per instance might be low enough for this technique not to add any benefit.

### Core usage in this demonstration

```java
// Baseline — each thread sends its own operate to Aerospike
client.operate(null, primaryKey, Operation.add(new Bin("unitsSold", 1)));

// Mitigation — reducer batches concurrent operates for the same key
HotKeyReducer reducer = new HotKeyReducer(
        client,
        Duration.ofMillis(reducerDelayMs),
        hotThreshold,
        hotDurationMs);

reducer.submit(null, primaryKey, Operation.add(new Bin("unitsSold", 1)));
```

The reducer is vendored from the standalone `hot-key-reducer` project. See `HotKeyReducer.getStatistics()` after the run for batching effectiveness (hot vs non-hot access counts).

## Trade-offs and When to Use

| Benefit | Cost |
|---------|------|
| Keeps a **single key** — no schema change | Only coalesces within **one JVM**; multiple app instances each need their own reducer (or a shared coalescing tier) |
| Dramatically cuts server round trips under burst load | Adds **latency** equal to the batch delay window for hot keys |
| Works with commutative ops like `add` | All batched operations must share the **same WritePolicy** (including filter expressions) |
| Transparent drop-in via `submit()` | Tuning-sensitive — thresholds affect when batching kicks in |
| Reduces `KEY_BUSY` without splitting data | Does not help if the server-side single `operate` itself is the bottleneck |
| One shared write policy per combined operation | Write policies must agree on items, especially the transaction the operations are in to be of use |

**Use in-process batching when:** many threads in the same process hammer one key, operations are batchable (increments, append, idempotent updates), you cannot shard the key, and you can tolerate milliseconds of additional latency on hot paths.

**Prefer shard + merge when:** you can split the logical counter and merge on read — it scales across all client instances without shared state.

**Prefer replica spread when:** the hot key is read-dominated rather than write-dominated.

> **REVIEW:** The reducer integration is functional but tuning-sensitive. Inspect `ReducerHotKeyUseCase` and `HotKeyReducer` before relying on specific batch sizes or latency in production.

## Demonstration

| Phase | Behaviour | What to expect |
|-------|-----------|----------------|
| **Baseline** | Each thread calls `client.operate` directly | Many server round trips; `KEY_BUSY` under high concurrency |
| **Mitigation** | Each thread calls `reducer.submit` | Fewer server transactions; reducer stats show hot-key batching |

After mitigation, the use case prints HotKeyReducer statistics and the final `unitsSold` on the primary record.

Example:

```bash
cd source && mvn compile exec:java \
  -Dexec.mainClass="com.aerospike.examples.UseCaseCookbookRunner" \
  -Dexec.args="-uc 'Hot Key - Write (HotKeyReducer)' -h localhost:3100 --param.numthreads=250 --param.transactionpendinglimit=5"
```

## Shared Data Model

```java
@Data
@AerospikeRecord(namespace = "test", set = "uccb_hotkey")
public class HotKeyProduct {
    @AerospikeKey
    private long id;
    private String sku;
    private String description;
    private int unitsSold;
}
```

- **Primary key:** `1` (fixed hot-key record — all traffic targets this single key)

Setup truncates the set and seeds the primary record. Replicas are not used.

## Simulation Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `numThreads` | 25 | Concurrent worker threads |
| `durationSecs` | 10 | Length of each phase |
| `transactionPendingLimit` | 20 | Namespace `transaction-pending-limit` applied via info `set-config` for the run (restored afterward) |
| `reducerDelayMs` | 1 | Wait before flushing a collected batch (ms) |
| `hotThreshold` | 3 | Accesses in the same millisecond before a key is treated as hot |
| `hotDurationMs` | 500 | How long a key stays hot after detection (ms) |

Every second the harness prints attempts, successes, `KEY_BUSY` errors, other errors, and average latency of **successful** operations (ms).

### Tuning `KEY_BUSY` visibility

Lower `transactionPendingLimit` makes hot-key rejection easier to observe in the **baseline** phase. The reducer mitigation should show fewer server-side transactions via batching. On single-node AP clusters, rw-hash contention may be limited — a multi-node replicated cluster is more realistic for reproducing write hot keys.

## Source Code Layout

```
hotkeys/
  ReducerHotKeyUseCase.java      This use case
  helper/HotKeyReducer.java      Vendored batching utility
  model/HotKeyProduct.java       Shared POJO
  HotKeyKeys.java                Primary key helpers
  HotKeyProductSetup.java        Truncate + seed
  HotKeySimulation.java          Thread pool + per-second stats
  HotKeySimulationParams.java    Shared parameters
  HotKeyPendingLimitScope.java   Apply/restore transaction-pending-limit
```
