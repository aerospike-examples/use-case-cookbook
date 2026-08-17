package com.aerospike.examples.transactionprocessing;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.examples.Async;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.transactionprocessing.model.Account;
import com.aerospike.examples.transactionprocessing.model.Transaction;
import com.aerospike.examples.transactionprocessing.model.TransactionMapper;

/**
 * SDK port of the legacy {@code TopTransactionsAcrossDcs} (see ../../java). Transactions can
 * arrive at either of two simulated DCs; each account keeps the most recent {@code
 * MAX_TRANSACTIONS} transaction ids per DC in a key-ordered map ({@code txns_dc1}/{@code
 * txns_dc2}, keyed by a zero-padded {@code "timestamp-id"} string so map key order == time order),
 * trimmed on every write. Reading the account's overall top transactions means merging both DC
 * maps and taking the most recent entries across both.
 * <p/>
 * The legacy version merges the two DC maps and takes the top N in one round trip via a nested
 * conditional expression ({@code Exp.cond} + {@code MapExp.putItems} + {@code
 * MapExp.getByIndexRange}). Given the nested-{@code MapExp} composition gap already found while
 * porting {@code TimeSeriesDemo} against this alpha SDK build, this port merges the two maps
 * client-side in {@link #getTopResults} instead of risking the same kind of nested-expression
 * failure.
 */
public class TopTransactionsAcrossDcs implements UseCase {

    private static final String BIN_DC1 = "txns_dc1";
    private static final String BIN_DC2 = "txns_dc2";
    private static final long NUM_ACCOUNTS = 1_000L;
    private static final int SIMULATION_DAYS = 30;
    private static final int MAX_TRANSACTIONS = 50;
    private static final long RUNTIME_SECS = 25L;

    private static final String[] DESCRIPTIONS = {"Grocery", "Gas station", "Online purchase", "ATM withdrawal", "Restaurant"};

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

    private DataSet accounts() {
        return DataSet.of(System.getProperty("demo.namespace", "test"), "uccb_account");
    }

    private DataSet transactions() {
        return DataSet.of(System.getProperty("demo.namespace", "test"), "uccb_txn");
    }

    public Key getAccountKey(String id) {
        return accounts().id(id);
    }

    public Key getTransactionKey(String id) {
        return transactions().id(id);
    }

    private String formMapKey(Transaction txn) {
        return String.format("%013d-%8s", txn.getTimestamp(), txn.getId());
    }

    @Override
    public void setup(Session session) throws Exception {
        DataSet accounts = accounts();
        session.truncate(accounts);
        session.truncate(transactions());

        System.out.printf("Generating %,d accounts%n", NUM_ACCOUNTS);
        for (long i = 1; i <= NUM_ACCOUNTS; i++) {
            session.upsert(accounts).object(new Account("acct-" + i)).execute();
        }
    }

    private Transaction randomTransaction(long counter) {
        String id = "txn-" + counter + "-" + ThreadLocalRandom.current().nextInt(1_000_000);
        String accountId = "acct-" + (ThreadLocalRandom.current().nextInt((int) NUM_ACCOUNTS) + 1);
        int amount = ThreadLocalRandom.current().nextInt(1, 100_000);
        Transaction.Status status = Transaction.Status.values()[ThreadLocalRandom.current().nextInt(Transaction.Status.values().length)];
        String desc = DESCRIPTIONS[ThreadLocalRandom.current().nextInt(DESCRIPTIONS.length)];
        String approvalCode = Long.toHexString(ThreadLocalRandom.current().nextLong());
        return new Transaction(id, 0L, amount, desc, status, null, approvalCode, accountId);
    }

    @Override
    public void run(Session session) throws Exception {
        Async.runFor(Duration.ofSeconds(RUNTIME_SECS), async -> {
            AtomicLong totalTxns = new AtomicLong();
            AtomicLong txnCounter = new AtomicLong();

            System.out.printf("Starting simulating time for %d seconds%n", RUNTIME_SECS);

            // We want the transactions to be roughly ordered to simulate real traffic patterns. Given the
            // execution should be short, we simulate the number of days of transactions we want over our run duration.
            async.useVirtualTime()
                    .elapse(Duration.ofDays(SIMULATION_DAYS))
                    .in(Duration.ofSeconds(RUNTIME_SECS))
                    .withPriorOffsetOf(Duration.ofDays(SIMULATION_DAYS))
                    .startingNow();

            System.out.println("Starting at: " + async.virtualDate());

            async.periodic(Duration.ofMillis(2500), () -> showTopTransactionsForAccount1(session, async, totalTxns));

            async.continuous(-1, () -> {
                Transaction txn = randomTransaction(txnCounter.incrementAndGet());
                String binName;
                if (async.rand().nextBoolean()) {
                    txn.setTimestamp(async.virtualTimeWithVariance(-15, 10));
                    binName = BIN_DC1;
                }
                else {
                    txn.setTimestamp(async.virtualTimeWithVariance(-100, -10));
                    binName = BIN_DC2;
                }
                txn.setOrigin(binName);
                session.upsert(transactions()).object(txn).execute();

                // Insert the new transaction into the account's DC map, and trim that map to the most recent MAX_TRANSACTIONS.
                session.upsert(getAccountKey(txn.getAccountId()))
                        .bin(binName).onMapKey(formMapKey(txn), MapOrder.KEY_ORDERED).upsert(txn.getId())
                        .bin(binName).onMapIndexRange(-MAX_TRANSACTIONS).removeAllOthers()
                        .execute();
                totalTxns.incrementAndGet();
            });
        });
    }

    private void showTopTransactionsForAccount1(Session session, Async async, AtomicLong totalTxns) {
        System.out.printf("%s: %,d transactions generated%n", async.virtualDate(), totalTxns.get());
        long now = System.currentTimeMillis();
        List<Transaction> topResults = getTopResults(session, MAX_TRANSACTIONS, "acct-1");
        long time = System.currentTimeMillis() - now;
        for (int i = 0; i < topResults.size(); i++) {
            Transaction txn = topResults.get(i);
            System.out.printf("%4d: %10s %8s %10s  %s  $%d%n",
                    i + 1, txn.getId(), txn.getAccountId(), txn.getOrigin(), new Date(txn.getTimestamp()), txn.getAmount());
        }
        System.out.printf("%d transaction(s) retrieved in %,dms%n%n", topResults.size(), time);
    }

    /**
     * Retrieves the most recent transactions for an account across both DC maps, merged and
     * sorted by their "timestamp-id" map key. Over-fetches by a few entries since a transaction's
     * map entry can arrive slightly before the transaction record itself.
     */
    @SuppressWarnings("unchecked")
    public List<Transaction> getTopResults(Session session, int count, String accountId) {
        int countToUse = count + 3;
        Optional<RecordResult> result = session.query(getAccountKey(accountId)).execute().getFirst();
        if (result.isEmpty() || !result.get().isOk()) {
            return List.of();
        }
        Record record = result.get().recordOrThrow();
        Map<String, ?> dc1 = (Map<String, ?>) record.getMap(BIN_DC1);
        Map<String, ?> dc2 = (Map<String, ?>) record.getMap(BIN_DC2);

        TreeMap<String, Object> merged = new TreeMap<>();
        if (dc1 != null) {
            merged.putAll(dc1);
        }
        if (dc2 != null) {
            merged.putAll(dc2);
        }

        List<String> txnIds = merged.descendingMap().values().stream()
                .limit(countToUse)
                .map(v -> (String) v)
                .collect(Collectors.toList());

        List<Key> keys = txnIds.stream().map(this::getTransactionKey).collect(Collectors.toList());
        TransactionMapper mapper = (TransactionMapper) session.getCluster().getRecordMappingFactory().getMapper(Transaction.class);

        List<Transaction> txns = new ArrayList<>();
        try (RecordStream stream = session.query(keys).execute()) {
            for (RecordResult r : stream.stream().toList()) {
                if (txns.size() >= count) {
                    break;
                }
                if (r.isOk()) {
                    Record rec = r.recordOrThrow();
                    txns.add(mapper.fromMap(rec.bins, r.getKey(), rec.generation));
                }
            }
        }
        return txns;
    }
}
