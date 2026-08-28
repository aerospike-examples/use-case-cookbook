package com.aerospike.examples.transactionprocessing;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.TypedDataSet;
import com.aerospike.client.sdk.TypedKey;
import com.aerospike.client.sdk.TypedKeyList;
import com.aerospike.client.sdk.TypedRecordStream;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.examples.Async;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.transactionprocessing.model.Account;
import com.aerospike.examples.transactionprocessing.model.Transaction;

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
 * MapExp.getByIndexRange}); an earlier pass at this port assumed the AEL equivalent wasn't
 * expressible (the same pre-canonical-reference conclusion later found wrong for {@link
 * com.aerospike.examples.timeseries.TimeSeriesDemo}'s device filter) and merged the two maps
 * client-side instead. Re-checked against the canonical AEL reference and it works - see {@link
 * #getTopResults} for the derivation, including the {@code let}-binding needed to select a range
 * on a merged map, and the {@code when}-gated fallback for when one or both DC bins don't exist
 * yet (an account with no transactions in a DC, common early in a run).
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

    private final TypedDataSet<Account> accounts =
            TypedDataSet.of(System.getProperty("demo.namespace", "test"), "uccb_account", Account.class);
    private final TypedDataSet<Transaction> transactions =
            TypedDataSet.of(System.getProperty("demo.namespace", "test"), "uccb_txn", Transaction.class);

    public TypedKey<Account> getAccountKey(String id) {
        return accounts.id(id);
    }

    public TypedKey<Transaction> getTransactionKey(String id) {
        return transactions.id(id);
    }

    private String formMapKey(Transaction txn) {
        return String.format("%013d-%8s", txn.getTimestamp(), txn.getId());
    }

    @Override
    public void setup(Session session) throws Exception {
        session.truncate(accounts);
        session.truncate(transactions);

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
                session.upsert(transactions).object(txn).execute();

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
     * <p/>
     * Merge-plus-top-N is a single AEL read: {@code putItems} merges {@code txns_dc2} into {@code
     * txns_dc1} (key-ordered, so the merge result is already sorted); the merged map can't be
     * selector-ranged directly ({@code $.a.putItems($.b).{-N:}} is a parse error - {@code
     * putItems()} is a path write terminal, so the chain ends there), but binding it to a {@code
     * let} variable and re-navigating from {@code (${var})} (canonical reference §4.2's rule for
     * continuing a path after a parenthesised expression) works. A {@code when} wraps the whole
     * thing to handle a DC bin not existing yet - {@code $.dc1.putItems($.dc2)} throws if either
     * bin is absent (bin navigation is strict by default, §4.1), which happens for any account
     * that hasn't received a transaction in one or both DCs.
     */
    @SuppressWarnings("unchecked")
    public List<Transaction> getTopResults(Session session, int count, String accountId) {
        int countToUse = count + 3;
        String ael = String.format(
                "when ("
                        + "$.%1$s.exists() and $.%2$s.exists() => (let (merged = $.%1$s.putItems($.%2$s)) then ((${merged}).{-%3$d:})), "
                        + "$.%1$s.exists() => $.%1$s.{-%3$d:}, "
                        + "$.%2$s.exists() => $.%2$s.{-%3$d:}, "
                        + "default => []"
                        + ")",
                BIN_DC1, BIN_DC2, countToUse);
        Record record = session.query(getAccountKey(accountId)).bin("top").selectFrom(ael).execute().getFirstRecord();
        if (record == null) {
            return List.of();
        }
        List<?> topAscending = record.getList("top");

        List<String> txnIds = new ArrayList<>();
        for (int i = topAscending.size() - 1; i >= 0; i--) {
            txnIds.add((String) topAscending.get(i));
        }
        if (txnIds.isEmpty()) {
            // No transactions have landed for this account yet (e.g. a display tick racing ahead
            // of the generator right at startup) - a batch query requires at least one key.
            return List.of();
        }

        TypedKeyList<Transaction> keys = new TypedKeyList<>();
        txnIds.forEach(id -> keys.add(getTransactionKey(id)));

        try (TypedRecordStream<Transaction> stream = session.query(keys).execute()) {
            List<Transaction> txns = stream.toObjectList();
            return txns.size() > count ? new ArrayList<>(txns.subList(0, count)) : txns;
        }
    }
}
