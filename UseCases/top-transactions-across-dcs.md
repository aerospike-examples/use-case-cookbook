# Time series data across multiple DCs
[Back to all use cases](../README.md)

[Link to working code](../source/src/main/java/com/aerospike/examples/transactionprocessing/TopTransactionsAcrossDcs.java)

## Use case
In a fraud detection scenario for financial services, the most recent transactions are the most important. In this use case, the finanical institution wants to retrieve the 50 most recent transactions for an account. The transactions will live in a table (set) by themselves and have an appropriate time-to-live on them, but retrieving the 50 most recent for an account in low single-digit milliseconds is needed to be able to power their fraud detection algorithms.

One interesting aspect of this use case is that transactions for the same account can be generated at exactly the same time (so the timestamp is not unique) and in two different regions at the same time. Transactions generated at a remote region will obviously take time to arrive, around 100ms or more across the USA. Hence:
- Transaction can arrive out of order, with more recent transactions arriving after older transactions
- The "latest" transactions must be measured by the timestamp of the transaction object, not the order the code sees them.

Note that if larger numbers of transactions were required, some of the techniques discussed here would work nicely with the [time series with large variance](timeseries-large-variance.md) use case. 

### Operations to be performed.

There are two different operations which need to be performed:

1. Save a transaction to the database
2. Retrieve the 50 most recent transactions for an account

Let's solve these first for a single data center (DC), then look at the changes we need to make for multiple DCs.

### Saving a transaction
There are two steps to saving a transaction:
1. Writing the transaction to the database
2. Updating a List or Map which holds the most recent transactions.

The generation of the transaction is handled by the `ValueCreator` class in the generator:

```java
ValueCreator<Transaction> transactionCreator = ValueCreatorCache.getInstance().get(Transaction.class);
Transaction txn = transactionCreator.createAndPopulate(generatorKeys);
mapper.save(txn);
```

The generator will process the annotations on the `Transaction` classs which tell it how to generate the data. Let's look at this class:

```java
@AerospikeRecord(namespace = "${demo.namespace:test}", set = "uccb_txn")
@Data
@GenMagic
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {
    public enum Status {
        APPROVED,
        DENIED,
        FRAUD
    }
    @AerospikeKey
    @GenExpression("'txn-' & $Key")
    private String id;
    private long timestamp;
    private int amount;
    private String desc;
    private Status status;
    @GenHexString(length = 12)
    private String approvalCode;
    @GenExpression("'acct-'& @GenNumber(start = 1, end = $NUM_ACCOUNTS)")
    private String accountId;
}
```

The `id` field will be generated with values `txn-1`, `txn-2`, etc, and the `accountId` will be in the format `acct-324` where the number is a random number between 1 and the number of accounts specified as a parameter to the use case. This ensures that the account is valid.

This code will generate a record in the database similar to:
```
{
    "accountId": "acct-432",
    "amount": 41,
    "approvalCode": "15208d018d06620ed96e2406",
    "desc": "iure commodi qui rem",
    "id": "txn-60949",
    "origin": "txns_dc1",
    "status": "APPROVED",
    "timestamp": 1753108939531
}
```

One subtlety we want to introduce is to do with the timestamp. Most of the time transactions will be received in roughtly increasing order. We could just generate a random timestamp -- the model will still support this -- but would make more sense for transaction timestamps to typically get larger as time progresses. We're only running the simulation for say 20 seconds, so how can we do that?

The use case running harness has an `Async` library as discussed in the [Leaderboard](leaderboard.md#running-the-demonstration) use case. This library not only supports simple concurrency models, but also has a concept of "virtual time". Let's say we want to simulate 30 days of transactions in a 25 second run. There are ~ 2.5 million seconds in 30 days, so 25 seconds needs to map to 2.5 million seconds. One second becomes 100,000 "virtual" seconds. So if we start the simulation at time X and a second has elapsed, this is 100,000 virutal seconds or about one day, 4 hours.

Luckily, all this maths is hidden within the `Async` library. You just need to tell it how long you actaully want to run for (25 seconds), how long a period do you want to simulate (30 days), and if you want to start before the current time, how long ago you want to start:

```java
async.useVirtualTime() 
    .elapse(Duration.ofDays(30))
    .in(Duration.ofSeconds(25))
    .withPriorOffsetOf(Duration.ofDays(30))
    .startingNow();
```

Once this has been set up the "virtual current time" can be retrieved using `async.virtualTime()` which returns the number of milliseconds since the Java epoch, or `async.virtualDate()` to return a `Java.util.Date` object. If you want some random variance in the result, for example due to network or processing delays, you can use `async.virtualTimeWithVariance(int min, int max)`. So to set the local timestamp, we will use:
```java
txn.setTimestamp(async.virtualTimeWithVariance(-15, 10));
```

### Updating the most recent transactions
We can store the most recent transactions in either a list or a map. Similar to what was discussed in the [timeseries](timeseries.md#filtering-by-time) use case, we will use a unique timestamp by combining the timestamp and the transaction id in a fixed-length String. This means that even if there are two transactions on the same account at exactly the same time, they will still not conflict with one another.

```java
private String formMapKey(Transaction txn) {
    return String.format("%013d-%8s", txn.getTimestamp(), txn.getId());
}
```

A list is the simplest to store these ids in, but a map is more flexible. This unique timestamp would be the map key, but the map value could be attributes used for further filtering, more details, etc. For example, the map value could be the transaction amount and the status of the transaction, so when the transaction details are returned to the client they can be filtered by relevant attributes before the results are gathered.

For this use case, we will use a map, but the value we store in the map will just be a dummy value (the transaction id). When we write the map will will perform two actions:

1. Insert our entry into the map
2. Trim the map to be contain only the 50 most recent entries.

In code this looks like:
```java
client.operate(null, getAccountKey(txn.getAccountId()),
        MapOperation.put(mapPolicy, binName, Value.get(formMapKey(txn)), Value.get(txn.getId())),
        MapOperation.removeByIndexRange(binName, -50, MapReturnType.INVERTED)
    );
```

The `removeByIndexRange` needs a bit of explaining. 

We're store the entries in a `KEY_ORDERED` map, with the key being our unique timestamp. Aerospike stores sorted maps in ascending order, so the most recent entries (with the highest timestamp) are at the end of the map. Thus, we only need the last 50 entries in the map. Aerospike allows negative indexes to be "from the end of the map", so we pass `-50` to say the last 50 entries in the map. We have not specified where this range ends, so Aerospike assumes we mean from this point to the end of the map. It's also smart enough to know that if there are fewer than 50 entries in the map to have the range being all the entries in the map.

So this gives us up to the 50 most recent entries. However, these are the items we want to __keep__, not remove. So we invert the selection using `MapReturnType.INVERTED`, which will trim anoth except those 50. This is exactly what we need.

### Reading the 50 most recent records
There are two steps to reading the 50 most recent transactions:
1. Reading the map of values
2. Performing a batch get to return all the records.

These steps are pretty easy:
```java
Key key = getAccountKey(accountId);
Record record = client.operate(null, key, ExpOperation.read("result", Exp.build(
    MapExp.getByIndexRange(MapReturnType.VALUE, Exp.val(0), Exp.mapBin("binName"))), 
    ExpReadFlags.DEFAULT));

if (record == null) {
    return List.of();
}
List<String> txnIds = (List<String>)record.getList("result");

Key[] keys = txnIds.stream()
        .map(txnId -> getTransactionKey(txnId))
        .toArray(Key[]::new);

BatchPolicy batchPolicy = client.copyBatchPolicyDefault();
batchPolicy.maxConcurrentThreads = 0;

Record[] records = client.get(batchPolicy, keys);
List<Transaction> txns = new ArrayList<>();
// Records are returned in ascending order, we need them in descending order.
for (int i = records.length-1; i >= 0; i--) {
    Record rec = records[i];
    if (records[i] != null) {
        txns.add(new Transaction(
                rec.getString("id"), 
                rec.getLong("timestamp"), 
                rec.getInt("amount"), 
                rec.getString("desc"), 
                Status.valueOf(rec.getString("status")), 
                rec.getString("origin"),
                rec.getString("approvalCode"), 
                rec.getString("accountId")
        ));
    }
}
return txns;
```

The `client.operate` just selects the value from the map for all entries in the map. The value contains the transaction id, which we can then use to turn into a key to read the transaction. We get an ordered list (smallest to biggest by timestamp) of these values, so we use a Java `stream` to turn these into an array of keys.

We do a batch read, allowing parallel reads across different servers, then turn each record back into a `Transaction` object to be returned to the client. Note that we probably want the list in descending order (most recent first) so we need to reverse the order of the array.

## Catering for multiple DCs
Everything shown to date has only been about solving the use case for one DC. Let's extend this to 2 DCs to see how this changes the data model.

### Changing the update
As mentioned earlier, XDR can set up "bin level shipping" so that two DCs can update the same record at the same time and guarantee eventual consistency, providing they __do not update the same bin__. If they update the same bin, they will converge to "last writer wins" semantics, definitely not what is desired here.

Let's have 2 separate maps, one per DC. Each DC writes their own map and ships it to the other DC, but because they're in separate bins they won't conflict. To save having to set up XDR in this scenario, we will just simulate it:

```java
Transaction txn = transactionCreator.createAndPopulate(generatorKeys);
String binName;
if (async.rand().nextBoolean()) {
    // Simulate variance in the timestamp from the local DC
    txn.setTimestamp(async.virtualTimeWithVariance(-15, 10));
    binName = BIN_DC1;
}
else {
    // Simulate variance in the timestamp from the remote DC
    txn.setTimestamp(async.virtualTimeWithVariance(-100, -10));
    binName = BIN_DC2;
}
txn.setOrigin(binName);
mapper.save(txn);

// Insert the new transaction into the account map, and truncate that map to 50
client.operate(null, getAccountKey(txn.getAccountId()),
        MapOperation.put(mapPolicy, binName, Value.get(formMapKey(txn)), Value.get(txn.getId())),
        MapOperation.removeByIndexRange(binName, -MAX_TRANSACTIONS.get(), MapReturnType.INVERTED)
    );
```
We will assume this code runs on DC1, so we're using the same variance we used above. But DC 2 is remote, so we've introduced a longer delay from 100ms to 10ms delay for cross-region traffic. The bin name is set to the bin for the source region, so we will update either map depending on which region we're simulating the transaction coming from. Other than that, the code is largely the same as the above, with the hard-coded number of transactions replaced by a parameter.

We have added a new field to the transaction, the origin. (DC1 or DC2). It doesn't make a difference, it's just for display purposes and probably won't be applicable in a real use case.

### Changing the read
The read process will need to change a bit. There are two maps, each of which has up to 50 entries in them. We want the 50 most recent transactions across both maps. We also need to make sure we handle the case where one map or the other (or both!) doesn't exist.

To get the most recent transactions across 2 maps, we will form them into one map by adding the contents of one map into the contents of another map in a `read` operation. Since it's a `read`, the cahnge will not result in the map being updated on the disk, just the working copy in memory.

```java
Exp joinDcBins = MapExp.putItems(mapPolicy, Exp.mapBin(BIN_DC1), Exp.mapBin(BIN_DC2))
```

This call returns a map which is the merging of the 2 maps. We want to take this result, and get the most recent 50:

```java
Exp getMostRecent = MapExp.getByIndexRange(MapReturnType.VALUE, Exp.val(-count), joinDcBins);
```

Note the `bin` parameter to the `getByIndexRange` call is not a bin like would be typically used, but rather is the map formed as a result of the `putItems` call. 

`count` is the number of records to return (50), but the write to the remote DC will result in the updated map bin and a new transaction record. Both of these updates are shipped independently so it is possible that the map has been updated but the transaction record has not arrived. 

To cater for this case, we're just going to get a few more of the most recent transactions, so if any of the records that are returned are `null`, there will be spare ones:

```java
// There's a very slim possibility that some of the transactions haven't arrived via XDR yet, so
// we want to over-estimate and filter it out later.
int countToUse = count + 3;
MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
Exp joinDcBins =
        Exp.cond(
                Exp.binExists(BIN_DC1),
                Exp.cond(
                        Exp.binExists(BIN_DC2),
                        MapExp.putItems(mapPolicy, Exp.mapBin(BIN_DC1), Exp.mapBin(BIN_DC2)),
                        Exp.mapBin(BIN_DC1)
                ),
                Exp.cond(
                        Exp.binExists(BIN_DC2),
                        Exp.mapBin(BIN_DC2),
                        Exp.val(Map.of())
                )
        );
        ;
Exp getMostRecent = MapExp.getByIndexRange(MapReturnType.VALUE, Exp.val(-countToUse), joinDcBins);
Key key = getAccountKey(accountId);
Record record = client.operate(null, key, ExpOperation.read("result", Exp.build(getMostRecent), ExpReadFlags.DEFAULT));
```
The `joinDcBins` looks a bit daunting, but really it's just checking for the existance of both bins, as the `putItems` call will fail if either bin is not there. What it's really saying in pseudo code is:
```
if bin DC1 exists then
    if bin DC2 exists then
        return putItems(bin DC1, bin DC2)
    else
        return bin DC1
    end if
else
    if bin DC2 exists then
        return bin DC2
    else
        return empty map
    end if
end if
```

Once we have the list of the 53 most recent items, we can load tehm easily. The only difference here is that we keep track of how many non-null 
```java
if (record == null) {
    return List.of();
}
List<String> txnIds = (List<String>)record.getList("result");

Key[] keys = txnIds.stream()
        .map(txnId -> getTransactionKey(txnId))
        .toArray(Key[]::new);

BatchPolicy batchPolicy = client.copyBatchPolicyDefault();
batchPolicy.maxConcurrentThreads = 0;

Record[] records = client.get(batchPolicy, keys);
List<Transaction> txns = new ArrayList<>();

// Records are returned in ascending order, we need them in descending order.
for (int i = records.length-1; i >= 0 && txn.size() < count; i--) {
    Record rec = records[i];
    if (records[i] != null) {
        txns.add(new Transaction(
                rec.getString("id"), 
                rec.getLong("timestamp"), 
                rec.getInt("amount"), 
                rec.getString("desc"), 
                Status.valueOf(rec.getString("status")), 
                rec.getString("origin"),
                rec.getString("approvalCode"), 
                rec.getString("accountId")
        ));
    }
}
return txns;
```

## Running the simulation
The whole `run` method is:
```java
Async.runFor(Duration.ofSeconds(RUN_DURATION_SECONDS.get()), async -> {
    ValueCreator<Transaction> transactionCreator = ValueCreatorCache.getInstance().get(Transaction.class);
    
    // Set the map parameters to allow transaction generation
    Map<String, Object> generatorKeys = Utils.getParamMap(this); 
    generatorKeys.put("Key", new AtomicLong(0));
    
    AtomicLong totalTxns = new AtomicLong();

    System.out.printf("Starting simulating time for %d seconds\n", RUN_DURATION_SECONDS.get());
    
    // We want the transactions to be roughly ordered to simulate real traffic patterns. Given the
    // execution should be short, we simulate the number of days of transactions we want over our run duration
    async.useVirtualTime() 
        .elapse(Duration.ofDays(SIMULATION_DAYS.get()))
        .in(Duration.ofSeconds(RUN_DURATION_SECONDS.get()))
        .withPriorOffsetOf(Duration.ofDays(SIMULATION_DAYS.get()))
        .startingNow();
    
    
    System.out.println("Starting at: " +async.virtualDate());
    
    // Every 2.5s, show the list of the top transactions for account 1.
    async.periodic(Duration.ofMillis(2500), () -> {
        showTopTransactionsForAccount1(async, totalTxns);
    });
    
    // Continuously update the records with many threads
    async.continuous(-1, () -> {
        Transaction txn = transactionCreator.createAndPopulate(generatorKeys);
        String binName;
        if (async.rand().nextBoolean()) {
            // Simulate variance in the timestamp from the local DC
            txn.setTimestamp(async.virtualTimeWithVariance(-15, 10));
            binName = BIN_DC1;
        }
        else {
            // Simulate variance in the timestamp from the remote DC
            txn.setTimestamp(async.virtualTimeWithVariance(-100, -10));
            binName = BIN_DC2;
        }
        txn.setOrigin(binName);
        mapper.save(txn);

        // Insert the new transaction into the account map, and truncate that map to 50
        client.operate(null, getAccountKey(txn.getAccountId()),
                MapOperation.put(mapPolicy, binName, Value.get(formMapKey(txn)), Value.get(txn.getId())),
                MapOperation.removeByIndexRange(binName, -MAX_TRANSACTIONS.get(), MapReturnType.INVERTED)
            );
        totalTxns.incrementAndGet();
    });
});
```
This basically does:
- Run the simulation for the desired time (20 seconds)
- Set up virtual time as discussed above
- Start a thread which every 2.5 seconds show the 50 most recent transactions for account 1
- Run (#processors - 1) threads which will continuously insert transactions into the transaction table across all accounts.

This will generate output like:
```
Wed Aug 06 19:30:18 AEST 2025: 186,738 transactions generated
   1: txn-184761   acct-1   txns_dc1  Wed Aug 06 13:46:05 AEST 2025  $220
   2: txn-184287   acct-1   txns_dc2  Wed Aug 06 12:25:56 AEST 2025  $815
   3: txn-181433   acct-1   txns_dc2  Wed Aug 06 04:25:05 AEST 2025  $532
   4: txn-181178   acct-1   txns_dc2  Wed Aug 06 03:43:18 AEST 2025  $206
   5: txn-179922   acct-1   txns_dc1  Wed Aug 06 00:07:04 AEST 2025  $364
   6: txn-178250   acct-1   txns_dc1  Tue Aug 05 19:17:06 AEST 2025  $487
   7: txn-176306   acct-1   txns_dc1  Tue Aug 05 13:43:10 AEST 2025  $301
   8: txn-175971   acct-1   txns_dc2  Tue Aug 05 12:45:41 AEST 2025  $287
   9: txn-174331   acct-1   txns_dc2  Tue Aug 05 08:05:08 AEST 2025  $793
  10: txn-173924   acct-1   txns_dc1  Tue Aug 05 06:55:21 AEST 2025  $172
  11: txn-173892   acct-1   txns_dc2  Tue Aug 05 06:50:10 AEST 2025  $206
  12: txn-173732   acct-1   txns_dc2  Tue Aug 05 06:22:20 AEST 2025  $94
  13: txn-172188   acct-1   txns_dc1  Tue Aug 05 01:50:57 AEST 2025  $958
  14: txn-171975   acct-1   txns_dc2  Tue Aug 05 01:12:33 AEST 2025  $594
  15: txn-171376   acct-1   txns_dc1  Mon Aug 04 23:30:20 AEST 2025  $296
  16: txn-170756   acct-1   txns_dc1  Mon Aug 04 21:40:24 AEST 2025  $710
  17: txn-170431   acct-1   txns_dc2  Mon Aug 04 20:45:31 AEST 2025  $824
  18: txn-168631   acct-1   txns_dc2  Mon Aug 04 15:09:43 AEST 2025  $543
  19: txn-168197   acct-1   txns_dc1  Mon Aug 04 13:57:39 AEST 2025  $106
  20: txn-167226   acct-1   txns_dc2  Mon Aug 04 11:13:59 AEST 2025  $518
  21: txn-167193   acct-1   txns_dc2  Mon Aug 04 11:08:26 AEST 2025  $258
  22: txn-164989   acct-1   txns_dc2  Mon Aug 04 05:01:26 AEST 2025  $130
  23: txn-164088   acct-1   txns_dc1  Mon Aug 04 02:24:57 AEST 2025  $506
  24: txn-161941   acct-1   txns_dc2  Sun Aug 03 19:58:32 AEST 2025  $957
  25: txn-161245   acct-1   txns_dc1  Sun Aug 03 17:56:18 AEST 2025  $555
  26: txn-159814   acct-1   txns_dc2  Sun Aug 03 14:01:14 AEST 2025  $76
  27: txn-159003   acct-1   txns_dc1  Sun Aug 03 11:45:45 AEST 2025  $393
  28: txn-157058   acct-1   txns_dc2  Sun Aug 03 06:21:29 AEST 2025  $242
  29: txn-155931   acct-1   txns_dc1  Sun Aug 03 03:09:21 AEST 2025  $48
  30: txn-153287   acct-1   txns_dc1  Sat Aug 02 19:38:03 AEST 2025  $358
  31: txn-153130   acct-1   txns_dc2  Sat Aug 02 19:12:23 AEST 2025  $567
  32: txn-152628   acct-1   txns_dc1  Sat Aug 02 17:44:07 AEST 2025  $933
  33: txn-152471   acct-1   txns_dc2  Sat Aug 02 17:16:58 AEST 2025  $465
  34: txn-151366   acct-1   txns_dc1  Sat Aug 02 13:59:52 AEST 2025  $76
  35: txn-151185   acct-1   txns_dc1  Sat Aug 02 13:28:00 AEST 2025  $460
  36: txn-150682   acct-1   txns_dc2  Sat Aug 02 11:51:25 AEST 2025  $609
  37: txn-149910   acct-1   txns_dc1  Sat Aug 02 09:35:33 AEST 2025  $968
  38: txn-149674   acct-1   txns_dc1  Sat Aug 02 08:54:57 AEST 2025  $301
  39: txn-149548   acct-1   txns_dc1  Sat Aug 02 08:33:50 AEST 2025  $669
  40: txn-149058   acct-1   txns_dc1  Sat Aug 02 07:10:11 AEST 2025  $99
  41: txn-147438   acct-1   txns_dc2  Sat Aug 02 02:18:05 AEST 2025  $387
  42: txn-147046   acct-1   txns_dc2  Sat Aug 02 01:06:20 AEST 2025  $857
  43: txn-146001   acct-1   txns_dc2  Fri Aug 01 21:59:41 AEST 2025  $195
  44: txn-143977   acct-1   txns_dc2  Fri Aug 01 15:37:56 AEST 2025  $579
  45: txn-143726   acct-1   txns_dc2  Fri Aug 01 14:52:44 AEST 2025  $949
  46: txn-142260   acct-1   txns_dc1  Fri Aug 01 10:23:54 AEST 2025  $449
  47: txn-141620   acct-1   txns_dc2  Fri Aug 01 08:31:23 AEST 2025  $506
  48: txn-141507   acct-1   txns_dc2  Fri Aug 01 08:10:25 AEST 2025  $267
  49: txn-140650   acct-1   txns_dc2  Fri Aug 01 05:41:48 AEST 2025  $695
  50: txn-139972   acct-1   txns_dc1  Fri Aug 01 03:45:45 AEST 2025  $184
50 transaction(s) retrieved in 4ms

Sat Aug 09 19:49:06 AEST 2025: 211,325 transactions generated
   1: txn-210404   acct-1   txns_dc2  Sat Aug 09 17:06:39 AEST 2025  $90
   2: txn-209513   acct-1   txns_dc2  Sat Aug 09 14:23:12 AEST 2025  $331
   3: txn-208202   acct-1   txns_dc1  Sat Aug 09 09:52:06 AEST 2025  $906
   4: txn-207361   acct-1   txns_dc1  Sat Aug 09 07:29:14 AEST 2025  $122
   5: txn-202691   acct-1   txns_dc1  Fri Aug 08 18:12:54 AEST 2025  $663
   6: txn-202642   acct-1   txns_dc2  Fri Aug 08 18:04:28 AEST 2025  $955
   7: txn-201040   acct-1   txns_dc1  Fri Aug 08 13:32:09 AEST 2025  $794
   8: txn-197795   acct-1   txns_dc1  Fri Aug 08 04:21:51 AEST 2025  $156
   9: txn-197405   acct-1   txns_dc2  Fri Aug 08 03:14:51 AEST 2025  $515
  10: txn-196996   acct-1   txns_dc1  Fri Aug 08 02:04:36 AEST 2025  $24
  11: txn-196846   acct-1   txns_dc1  Fri Aug 08 01:37:07 AEST 2025  $979
  12: txn-196326   acct-1   txns_dc1  Fri Aug 08 00:05:24 AEST 2025  $825
  13: txn-195199   acct-1   txns_dc1  Thu Aug 07 20:57:50 AEST 2025  $808
  14: txn-195184   acct-1   txns_dc2  Thu Aug 07 20:54:58 AEST 2025  $509
  15: txn-190477   acct-1   txns_dc1  Thu Aug 07 06:49:11 AEST 2025  $179
  16: txn-190397   acct-1   txns_dc1  Thu Aug 07 06:36:07 AEST 2025  $590
  17: txn-187641   acct-1   txns_dc2  Wed Aug 06 22:13:16 AEST 2025  $533
  18: txn-184761   acct-1   txns_dc1  Wed Aug 06 13:46:05 AEST 2025  $220
  19: txn-184287   acct-1   txns_dc2  Wed Aug 06 12:25:56 AEST 2025  $815
  20: txn-181433   acct-1   txns_dc2  Wed Aug 06 04:25:05 AEST 2025  $532
  21: txn-181178   acct-1   txns_dc2  Wed Aug 06 03:43:18 AEST 2025  $206
  22: txn-179922   acct-1   txns_dc1  Wed Aug 06 00:07:04 AEST 2025  $364
  23: txn-178250   acct-1   txns_dc1  Tue Aug 05 19:17:06 AEST 2025  $487
  24: txn-176306   acct-1   txns_dc1  Tue Aug 05 13:43:10 AEST 2025  $301
  25: txn-175971   acct-1   txns_dc2  Tue Aug 05 12:45:41 AEST 2025  $287
  26: txn-174331   acct-1   txns_dc2  Tue Aug 05 08:05:08 AEST 2025  $793
  27: txn-173924   acct-1   txns_dc1  Tue Aug 05 06:55:21 AEST 2025  $172
  28: txn-173892   acct-1   txns_dc2  Tue Aug 05 06:50:10 AEST 2025  $206
  29: txn-173732   acct-1   txns_dc2  Tue Aug 05 06:22:20 AEST 2025  $94
  30: txn-172188   acct-1   txns_dc1  Tue Aug 05 01:50:57 AEST 2025  $958
  31: txn-171975   acct-1   txns_dc2  Tue Aug 05 01:12:33 AEST 2025  $594
  32: txn-171376   acct-1   txns_dc1  Mon Aug 04 23:30:20 AEST 2025  $296
  33: txn-170756   acct-1   txns_dc1  Mon Aug 04 21:40:24 AEST 2025  $710
  34: txn-170431   acct-1   txns_dc2  Mon Aug 04 20:45:31 AEST 2025  $824
  35: txn-168631   acct-1   txns_dc2  Mon Aug 04 15:09:43 AEST 2025  $543
  36: txn-168197   acct-1   txns_dc1  Mon Aug 04 13:57:39 AEST 2025  $106
  37: txn-167226   acct-1   txns_dc2  Mon Aug 04 11:13:59 AEST 2025  $518
  38: txn-167193   acct-1   txns_dc2  Mon Aug 04 11:08:26 AEST 2025  $258
  39: txn-164989   acct-1   txns_dc2  Mon Aug 04 05:01:26 AEST 2025  $130
  40: txn-164088   acct-1   txns_dc1  Mon Aug 04 02:24:57 AEST 2025  $506
  41: txn-161941   acct-1   txns_dc2  Sun Aug 03 19:58:32 AEST 2025  $957
  42: txn-161245   acct-1   txns_dc1  Sun Aug 03 17:56:18 AEST 2025  $555
  43: txn-159814   acct-1   txns_dc2  Sun Aug 03 14:01:14 AEST 2025  $76
  44: txn-159003   acct-1   txns_dc1  Sun Aug 03 11:45:45 AEST 2025  $393
  45: txn-157058   acct-1   txns_dc2  Sun Aug 03 06:21:29 AEST 2025  $242
  46: txn-155931   acct-1   txns_dc1  Sun Aug 03 03:09:21 AEST 2025  $48
  47: txn-153287   acct-1   txns_dc1  Sat Aug 02 19:38:03 AEST 2025  $358
  48: txn-153130   acct-1   txns_dc2  Sat Aug 02 19:12:23 AEST 2025  $567
  49: txn-152628   acct-1   txns_dc1  Sat Aug 02 17:44:07 AEST 2025  $933
  50: txn-152471   acct-1   txns_dc2  Sat Aug 02 17:16:58 AEST 2025  $465
50 transaction(s) retrieved in 6ms
```
You can see in the 25 seconds, 211,325 transaction records were written / accounts updated. In the first set of results, the most recent transaction was `txn-184761` but in the second set of results, this had fallen down to the 18th most recent transaction. There is also a healthy mix of transactions from both DCs.