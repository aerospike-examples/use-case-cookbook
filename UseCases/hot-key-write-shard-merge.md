# Hot Key — Write (Shard + Merge)
[Back to all use cases](../README.md)

Related patterns: [Read (Replica Spread)](hot-key-read-replica-spread.md) · [Write (HotKeyReducer)](hot-key-write-reducer.md)

[Link to working code](../source/src/main/java/com/aerospike/examples/hotkeys/WriteHotKeyUseCase.java)

## The Problem

Write hot keys are common in counters, rate limiters, inventory decrements, and analytics — anywhere many clients update the **same record** concurrently. All increments target one key, so:

- Every write contends on the **same record lock** and the same rw-hash pending queue.
- When pending transactions exceed `transaction-pending-limit` (default 20), Aerospike returns `KEY_BUSY`. Below the limit, writes queue and latency grows — often long before errors appear.
- On a single-node AP cluster, many simple writes may never enter the rw-hash path that drives `KEY_BUSY` statistics, so you see latency inflation without obvious errors. Multi-node clusters with replication are more representative.

Unlike read hot keys, write hot keys are usually easy to spot: `fail_key_busy` / `err_rw_pending_limit` counters rise, and clients log error 14.

## How the Technique Works

**Shard + merge** removes the write hot key by splitting one logical counter across **N physical records**, so no single record receives all the write pressure.

1. **Shard the counter at write time.** Instead of incrementing primary key `1`, each writer picks a random shard key — `1:0`, `1:1`, … `1:N-1` — and runs `Operation.add` on that shard only. With uniform random selection, each shard receives roughly **1/N** of the write load.
2. **Merge at read time.** The logical total is the **sum** of `unitsSold` across all shard records. Aerospike batch reads (`client.get(null, keys[], bin)`) fetch all shards in one round trip and sum them.
3. **No single record is hot** as long as shard count scales with write concurrency. The trade-off moves contention from writes (cheap individually) to the occasional merge read (reads N keys).

```
  Baseline                         Mitigation

  250 threads ──► ┌──────────┐     250 threads ──┬──► ┌──────────┐
                  │  key: 1  │                   ├──► │  1:0     │
                  │ unitsSold│                   ├──► │  1:1     │
                  └──────────┘                   ├──► │  1:2     │
                     ▲ hot                       └──► │  1:3     │
                                                      └──────────┘
                                                      merge read sums all shards
```

Because `KEY_BUSY` is enforced **per record**, sharding divides both queue depth and rw-hash contention by roughly the shard count. The mitigation phase in this demonstration should show dramatically fewer `KEY_BUSY` errors than the baseline at the same thread count.

### Core write and merge paths

```java
// Baseline — every increment hits one record
client.operate(null, primaryKey, Operation.add(new Bin("unitsSold", 1)));

// Mitigation — increment a random shard
Key shardKey = HotKeyKeys.randomReplica(mapper, productId, replicaCount);
client.operate(null, shardKey, Operation.add(new Bin("unitsSold", 1)));

// Merge — reconstruct the logical total when needed
Key[] keys = HotKeyKeys.allReplicaKeys(mapper, productId, replicaCount).toArray(new Key[0]);
Record[] records = client.get(null, keys, "unitsSold");
int total = 0;
for (Record record : records) {
    if (record != null) {
        total += record.getInt("unitsSold");
    }
}
```

The merge logic lives in [HotKeyProductSetup.readMergedUnitsSold](../source/src/main/java/com/aerospike/examples/hotkeys/HotKeyProductSetup.java).

### Why this works for counters

`Operation.add` is commutative and associative: `sum(shard[0..N-1])` equals what a single counter would have held if all increments had landed on one key. You lose a single atomic global counter, but gain **N independent write paths**. For view counts, likes, or throughput meters where an approximate or eventually consistent total is acceptable at read time, this is a proven pattern.

## Trade-offs and When to Use

| Benefit | Cost |
|---------|------|
| Write throughput scales ~linearly with shard count | Logical total requires a **merge read** of N keys |
| Each shard has its own rw-hash queue | Totals are not atomically up to date during concurrent writes |
| Simple `add` operations — no custom UDF required | Shard count must be chosen up front; resharding requires migration |
| Works across client instances — no shared in-process state | Primary key `1` in this demo is unused during mitigation writes |

**Use shard + merge when:** you have a write-heavy counter or accumulator, reads of the total are less frequent than increments, and slight lag on the merged total is acceptable (page views, API call counts, download tallies).

**Avoid when:** you need a strictly atomic global value on every read, or the merge read itself would become hot (millions of clients all merging at once — consider caching the merged total or using a read replica spread on top).

This demonstration also reads the merged total ~every 100ms during both phases to mix periodic reads with write-heavy traffic, mimicking a dashboard polling the counter while events stream in.

## Demonstration

| Phase | Behaviour | What to expect |
|-------|-----------|----------------|
| **Baseline** | All threads increment primary key `1` | High `KEY_BUSY` and latency with many threads and a low pending limit |
| **Mitigation** | Each thread increments a random shard `1:0..N-1` | Far fewer `KEY_BUSY` errors; similar or better throughput |

After mitigation, the use case prints the merged `unitsSold` total across all shards and compares success counts between phases.

Try a dramatic baseline with many threads and a low pending limit:

```bash
cd source && mvn compile exec:java \
  -Dexec.mainClass="com.aerospike.examples.UseCaseCookbookRunner" \
  -Dexec.args="-uc 'Hot Key - Write (Shard + Merge)' -h localhost:3100 --param.numthreads=250 --param.transactionpendinglimit=5"
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

- **Primary key:** `1` (used in baseline; seeded but not written in mitigation phase)
- **Shard keys:** `1:0`, `1:1`, … (`replicaCount` default `4`)

Setup truncates the set and writes identical starting records to the primary key and each shard.

## Simulation Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `numThreads` | 25 | Concurrent worker threads |
| `durationSecs` | 10 | Length of each phase |
| `replicaCount` | 4 | Number of shard records |
| `transactionPendingLimit` | 20 | Namespace `transaction-pending-limit` applied via info `set-config` for the run (restored afterward) |

Every second the harness prints attempts, successes, `KEY_BUSY` errors, other errors, and average latency of **successful** operations (ms). When other errors occur, a breakdown by error type is printed at the end of each phase.

### Tuning `KEY_BUSY` visibility

Lower `transactionPendingLimit` makes hot-key rejection easier to observe in the **baseline** phase. The mitigation phase spreads writes across shards, so `KEY_BUSY` rates should drop accordingly. Setting the limit to **0** disables the queue check entirely (latency only, no errors).

## Source Code Layout

```
hotkeys/
  WriteHotKeyUseCase.java        This use case
  model/HotKeyProduct.java       Shared POJO
  HotKeyKeys.java                Primary and shard key helpers
  HotKeyProductSetup.java        Truncate + seed + merge read
  HotKeySimulation.java          Thread pool + per-second stats
  HotKeySimulationParams.java    Shared parameters
  HotKeyPendingLimitScope.java   Apply/restore transaction-pending-limit
```
