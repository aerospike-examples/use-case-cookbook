# Hot Key — Read (Replica Spread)
[Back to all use cases](../README.md)

Related patterns: [Write (Shard + Merge)](hot-key-write-shard-merge.md) · [Write (HotKeyReducer)](hot-key-write-reducer.md)

[Link to working code](../source/src/main/java/com/aerospike/examples/hotkeys/ReadHotKeyUseCase.java)

## The Problem

A **hot key** occurs when disproportionate traffic targets a single Aerospike record. Every read for that logical object goes to the same primary key, which means:

- All requests hash to the **same partition** and are handled by the **same cluster node**.
- On a strongly consistent (SC) namespace, reads and writes for that record are serialized through the **rw-hash** — a per-record queue with a configurable `transaction-pending-limit` (default 20). When the queue fills, the server returns `KEY_BUSY` (error 14); below the limit, requests wait and latency rises.
- On an AP namespace, read hot keys are harder to spot — there is no simple statistic for concurrent read pressure in the way `fail_key_busy` tracks write-side rw-hash contention. Latency inflation on the owning node is often the first symptom.

Typical causes include a viral product page, a leaderboard top entry, a session record everyone reads, or a configuration blob fetched on every request.

In Aerospike, read hot keys can be difficult to identify when running in AP mode. In SC mode, read transactions are serialized through the rw-hash along with write transactions. To see `KEY_BUSY` effectively in this demonstration, run against an **SC cluster** with a low `transactionPendingLimit` (for example 2). If you suspect a read hot key but cannot confirm it, see [How to identify read hotkeys](https://support.aerospike.com/hc/en-us/articles/49939303451547-How-To-identify-read-hotkeys).

## How the Technique Works

**Replica spread** removes the hot key by giving each reader an alternative record to read instead of hammering one key.

1. **Seed identical copies** of the same data at separate keys — for example primary key `1` and replicas `1:0`, `1:1`, `1:2`, `1:3`. Each key is a distinct Aerospike record with its own digest. Depending on partition count, replicas may land on different partitions and even different nodes, further spreading network and CPU load.
2. **On read, pick a replica at random** (or round-robin, or hash the client id modulo N). Each reader still gets the same logical data, but concurrent reads are divided roughly by the number of replicas.
3. **On write, refresh every copy in one batch write.** When the source data changes, apply the same update to the primary key and all replicas in a single batch `operate` call. This keeps replicas consistent without readers needing to know which copy is authoritative.

```
                    ┌──────────┐
  Baseline          │  key: 1  │  ← all N readers
                    └──────────┘

        ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
        │  1:0     │ │  1:1     │ │  1:2     │ │  1:3     │
        └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘
             │            │            │            │
  Mitigation └────────────┴────────────┴────────────┘
             each reader picks one replica at random
```

Because `KEY_BUSY` and rw-hash contention are **per record**, splitting reads across four records divides the per-record queue depth by roughly four. The simulation's mitigation phase should show lower latency and fewer `KEY_BUSY` errors than the baseline for the same thread count.

### Core read path

```java
// Baseline — every thread hits the same record
client.get(null, primaryKey, "sku", "description", "unitsSold");

// Mitigation — load spread across replica keys
Key replicaKey = HotKeyKeys.randomReplica(mapper, productId, replicaCount);
client.get(null, replicaKey, "sku", "description", "unitsSold");
```

Replica keys are constructed in [HotKeyKeys.java](../source/src/main/java/com/aerospike/examples/hotkeys/HotKeyKeys.java) as `productId + ":" + index`, giving each replica a unique user key and digest.

### Keeping replicas in sync

When a write occurs, update the primary and every replica in one batch write:

```java
Key[] keys = HotKeyKeys.primaryAndAllReplicaKeys(mapper, productId, replicaCount);
BatchPolicy batchPolicy = client.copyBatchPolicyDefault();
// aerospike-client-jdk8 defaults maxConcurrentThreads to 1; use 0 for parallel sub-requests.
// Not required with aerospike-client-jdk21.
batchPolicy.maxConcurrentThreads = 0;
client.operate(
        batchPolicy,
        client.copyBatchWritePolicyDefault(),
        keys,
        Operation.add(new Bin("unitsSold", 1)));
```

This is implemented in [HotKeyProductSetup.incrementUnitsSoldOnAllCopies](../source/src/main/java/com/aerospike/examples/hotkeys/HotKeyProductSetup.java) and invoked ~every 100ms during both simulation phases as background refresh traffic alongside the read load.

## Trade-offs and When to Use

| Benefit | Cost |
|---------|------|
| Simple to implement — no server-side changes | Replicas can be **stale** unless you refresh them on every write |
| Works with plain `get` — no batch reads required | Storage multiplied by replica count |
| Scales read QPS roughly linearly with replica count | Writes become N times more expensive if you update every replica synchronously |
| Good fit for **read-heavy, rarely changing** data | Not suitable when every read must see the latest write |

**Use replica spread when:**

- **Make sure this is really needed** — most of the time Aerospike does not suffer from read hot keys.
- The data changes infrequently relative to reads, or you can publish updates to all replicas in the write path (as this demonstration does with batch writes).

**Avoid when:** readers require strict read-your-writes consistency without waiting for replica refresh, or the write rate to keep all replicas in sync would itself create write hot keys across every copy.

## Demonstration

The use case runs two comparable phases so you can see the technique working:

| Phase | Behaviour | What to expect |
|-------|-----------|----------------|
| **Baseline** | All threads call `get` on primary key `1` | Higher latency; more `KEY_BUSY` on SC clusters with a low pending limit |
| **Mitigation** | Each thread calls `get` on a random replica `1:0..N-1` | Lower latency; fewer `KEY_BUSY` errors at the same thread count |

Both phases also batch-write an increment to `unitsSold` on the primary key **and every replica** (~every 100ms) so all copies stay in sync while read load dominates.

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

- **Primary key:** `1` (fixed hot-key record)
- **Replica keys:** `1:0`, `1:1`, … (`replicaCount` default `4`)

Setup truncates the set and writes identical product records to the primary key and each replica.

## Simulation Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `numThreads` | 25 | Concurrent worker threads |
| `durationSecs` | 10 | Length of each phase |
| `replicaCount` | 4 | Number of replica records |
| `transactionPendingLimit` | 20 | Namespace `transaction-pending-limit` applied via info `set-config` for the run (restored afterward) |

Every second the harness prints attempts, successes, `KEY_BUSY` errors, other errors, and average latency of **successful** operations (ms). A cumulative total line is printed when each phase completes.

Example:

```bash
cd source && mvn compile exec:java \
  -Dexec.mainClass="com.aerospike.examples.UseCaseCookbookRunner" \
  -Dexec.args="-uc 'Hot Key - Read (Replica Spread)' -h localhost:3100 --param.numthreads=25 --param.durationsecs=10"
```

### Tuning `KEY_BUSY` visibility

`KEY_BUSY` (error 14) is returned when concurrent operations on the **same record** exceed the namespace `transaction-pending-limit` (default **20**). Below that limit, Aerospike queues requests and you see **latency increase** rather than errors.

Lower `transactionPendingLimit` makes hot-key rejection easier to observe — for example `--param.transactionpendinglimit=5 --param.numthreads=250`. Setting the limit to **0** disables the queue check entirely.

**Single-node / AP clusters:** On an AP namespace with one node, many simple operations never enter the rw-hash path that drives `KEY_BUSY`. Even with a low limit you may see latency inflation before many `KEY_BUSY` errors. A multi-node SC cluster with replication is the most realistic environment for reproducing read-side hot-key failures.

## Source Code Layout

```
hotkeys/
  ReadHotKeyUseCase.java         This use case
  model/HotKeyProduct.java       Shared POJO
  HotKeyKeys.java                Primary and replica key helpers
  HotKeyProductSetup.java        Truncate + seed
  HotKeySimulation.java          Thread pool + per-second stats
  HotKeySimulationParams.java    Shared parameters
  HotKeyPendingLimitScope.java   Apply/restore transaction-pending-limit
```
