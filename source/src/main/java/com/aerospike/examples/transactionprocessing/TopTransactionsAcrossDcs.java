package com.aerospike.examples.transactionprocessing;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.examples.Async;
import com.aerospike.examples.Parameter;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.Utils;
import com.aerospike.examples.transactionprocessing.datamodel.Account;
import com.aerospike.examples.transactionprocessing.datamodel.Transaction;
import com.aerospike.examples.transactionprocessing.datamodel.Transaction.Status;
import com.aerospike.generator.Generator;
import com.aerospike.generator.ValueCreator;
import com.aerospike.generator.ValueCreatorCache;
import com.aerospike.mapper.tools.AeroMapper;

public class TopTransactionsAcrossDcs implements UseCase {
    public static final String BIN_DC1 = "txns_dc1";
    public static final String BIN_DC2 = "txns_dc2";
    public static final Parameter<Long> NUM_ACCOUNTS = new Parameter<>("NUM_ACCOUNTS", 1_000l, "Number of accounts to use");
    public static final Parameter<Integer> SIMULATION_DAYS = new Parameter<>("SIMULATION_DAYS", 30, "How many days to cover in the simulation");
    public static final Parameter<Integer> MAX_TRANSACTIONS = new Parameter<>("MAX_TRANSACTIONS", 50, "How many most recent transactions to retrieve");
    public static final Parameter<Long> RUN_DURATION_SECONDS = new Parameter<>("RUN_DURATION_SECONDS", 25L, "The duration to run the simulation for.");
    
    private static final MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
    private IAerospikeClient client;
    private String namespace;
    private String accountSetName;
    private String transactionSetName;
    
    @Override
    public String getName() {
        return "Top 50 Transaction Across DCs";
    }

    @Override
    public String getDescription() {
        return "Find the top 50 transactions for an account. Transactions can arrive at either of 2 DCs at any point in time. "
                + "Transaction shipping from the remote DC is about 100ms. Transactions have a unique id but 2 transaction can arrive for the same "
                + "account at the same time.";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/top-transactions-across-dcs.md";
    }
    
    @Override
    public String[] getTags() {
        return new String[] {"XDR", "Nested Expressions", "XDR Map"}; 
    }
    
    @Override
    public Parameter<?>[] getParams() {
        return new Parameter<?>[] {NUM_ACCOUNTS, SIMULATION_DAYS, MAX_TRANSACTIONS, RUN_DURATION_SECONDS};
    }

    @Override
    public void setup(IAerospikeClient client, AeroMapper mapper) throws Exception {
        this.client = client;
        namespace = mapper.getNamespace(Account.class);
        accountSetName = mapper.getSet(Account.class);
        transactionSetName = mapper.getSet(Transaction.class);
        this.client.truncate(null, namespace, accountSetName, null);
        this.client.truncate(null, namespace, transactionSetName, null);
        new Generator()
            .generate(1, NUM_ACCOUNTS.get(), Account.class, Utils.getParamMap(this), mapper::save)
            .monitor();
    }

    public Key getAccountKey(String id) {
        return new Key(namespace, accountSetName, id);
    }
    
    public Key getTransactionKey(String id) {
        return new Key(namespace, transactionSetName, id);
    }
    
    private String formMapKey(Transaction txn) {
        return String.format("%013d-%8s", txn.getTimestamp(), txn.getId());
    }
    
    private void showTopTransactionsForAccount1(Async async, AtomicLong totalTxns) {
        System.out.printf("%s: %,d transactions generated%n", async.virtualDate(), totalTxns.get());
        long now = System.currentTimeMillis();
        List<Transaction> topResults = getTopResults(MAX_TRANSACTIONS.get(), "acct-1");
        long time = System.currentTimeMillis() - now;
        for (int i = 0; i < topResults.size(); i++) {
            Transaction txn = topResults.get(i);
            System.out.printf("%4d: %10s %8s %10s  %s  $%d%n",
                    i+1, txn.getId(), txn.getAccountId(), txn.getOrigin(), new Date(txn.getTimestamp()), txn.getAmount());
        }
        System.out.printf("%d transaction(s) retrieved in %,dms%n%n", topResults.size(), time);
    }
    
    @Override
    public void run(IAerospikeClient client, AeroMapper mapper) throws Exception {
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
    }
    
    /**
     * Retrieve the top records (most recent by time) for the account. All transactions are stored, but the most recent
     * by time are stored in maps. There are two of these maps, one per DC, so they can be shipped independently using bin
     * shipping. This code caters for these multiple maps by merging the maps together in memory, then selecting the top N
     * records from this merged map.
     * <p>
     * Note that as the transactions and map records get shipped independently via XDR, there is a possibility that we will
     * get the entry in the map before the transaction reaches us. Due to this, we select a few more records, allowing for
     * the possibility of {@code null} to be returned for a couple of records.
     * @param count
     * @param accountId
     * @return
     */
    @SuppressWarnings("unchecked")
    public List<Transaction> getTopResults(int count, String accountId) {
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
        for (int i = records.length-1; i >= 0 && txns.size() < count; i--) {
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
    }
}
