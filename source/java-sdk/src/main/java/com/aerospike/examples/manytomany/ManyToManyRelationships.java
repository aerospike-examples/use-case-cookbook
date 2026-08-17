package com.aerospike.examples.manytomany;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Session;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.manytomany.model.Account;
import com.aerospike.examples.manytomany.model.Customer;

/**
 * SDK port of the legacy {@code ManyToManyRelationships} (see ../../java). Demonstrates a
 * many-to-many relationship: each account holds a set-like list of owning customer ids
 * ({@code owners}), and each customer holds a set-like list of the accounts it owns
 * ({@code accounts}), so the relationship can be traversed from either side.
 * <p/>
 * The legacy version's {@code addAccount} builds a {@code BatchPolicy} with {@code txn} set, then
 * immediately overwrites it back to {@code null} before the batch call - so that step never
 * actually ran inside the transaction there. This port fixes that by running the whole thing
 * (account write, owners bin, and the batch update of every owning customer) against the same
 * transactional session.
 */
public class ManyToManyRelationships implements UseCase {

    private static final int NUM_ACCOUNTS = 2_000;
    private static final int NUM_CUSTOMERS = 1_000;

    private static final String[] FIRST_NAMES = {"Estefana", "Arnoldo", "Jacquelyne", "Harris", "Ardelia"};
    private static final String[] LAST_NAMES = {"Ruecker", "MacGyver", "Willms", "Jones", "Renner"};

    @Override
    public String getName() {
        return "Many to many relationships";
    }

    @Override
    public String getDescription() {
        return "Demonstrate how to handle many-to-many relationships in Aerospike. Traversing relationships in both "
                + "directions and adding entities are discussed.";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/many-to-many-relationships.md";
    }

    private DataSet accounts() {
        return DataSet.of(System.getProperty("demo.namespace", "test"), "uccb_account");
    }

    private DataSet customers() {
        return DataSet.of(System.getProperty("demo.namespace", "test"), "uccb_customer");
    }

    private Customer randomCustomer(String custId) {
        String first = FIRST_NAMES[ThreadLocalRandom.current().nextInt(FIRST_NAMES.length)];
        String last = LAST_NAMES[ThreadLocalRandom.current().nextInt(LAST_NAMES.length)];
        long fiftyYearsMs = 50L * 365 * 24 * 60 * 60 * 1000;
        long tenYearsMs = 10L * 365 * 24 * 60 * 60 * 1000;
        Date dob = new Date(System.currentTimeMillis() - ThreadLocalRandom.current().nextLong(fiftyYearsMs / 2, fiftyYearsMs));
        Date dateJoined = new Date(System.currentTimeMillis() - ThreadLocalRandom.current().nextLong(0, tenYearsMs));
        return new Customer(custId, first, last, dob, dateJoined);
    }

    private Account randomAccount(String id) {
        int balanceInCents = ThreadLocalRandom.current().nextInt(500, 2_000_000);
        long fiveYearsMs = 5L * 365 * 24 * 60 * 60 * 1000;
        Date dateOpened = new Date(System.currentTimeMillis() - ThreadLocalRandom.current().nextLong(0, fiveYearsMs));
        return new Account(id, "Account " + id, balanceInCents, dateOpened);
    }

    /**
     * Adds a new account and updates every owning customer's {@code accounts} list, all inside a
     * transaction. Returns {@code true} if every operation succeeded.
     */
    public boolean addAccount(Session session, Account account, List<String> ownerIds) {
        return session.doInTransactionReturning(tx -> {
            tx.upsert(accounts()).object(account).execute();
            tx.upsert(accounts().id(account.getId())).bin("owners").setTo(ownerIds).execute();

            List<Key> customerKeys = ownerIds.stream().map(id -> customers().id(id)).collect(Collectors.toList());
            try (RecordStream stream = tx.upsert(customerKeys)
                    .bin("accounts").listAppend(account.getId(), opts -> opts.addUnique().allowFailures())
                    .execute()) {
                return stream.stream().allMatch(RecordResult::isOk);
            }
        });
    }

    @Override
    public void setup(Session session) throws Exception {
        DataSet accounts = accounts();
        DataSet customers = customers();
        session.truncate(accounts);
        session.truncate(customers);

        System.out.println("Generating Customers");
        for (int i = 1; i <= NUM_CUSTOMERS; i++) {
            session.upsert(customers).object(randomCustomer("Cust-" + i)).execute();
        }

        System.out.println("Generating Accounts");
        for (int i = 1; i <= NUM_ACCOUNTS; i++) {
            Account account = randomAccount(UUID.randomUUID().toString());
            int numOwners = ThreadLocalRandom.current().nextInt(1, 8);
            List<String> ownerIds = IntStream.range(0, numOwners)
                    .mapToObj(n -> "Cust-" + (ThreadLocalRandom.current().nextInt(NUM_CUSTOMERS) + 1))
                    .distinct()
                    .sorted()
                    .collect(Collectors.toList());
            addAccount(session, account, ownerIds);
        }
    }

    /**
     * Determines every customer related to {@code customerId} - i.e. sharing ownership of at
     * least one account - and how many accounts they share. Returns a map of related customer id
     * to shared-account count.
     */
    public Map<String, Integer> getRelatedCustomers(Session session, String customerId) {
        Optional<RecordResult> customerResult = session.query(customers().id(customerId))
                .readingOnlyBins("accounts").execute().getFirst();
        if (customerResult.isEmpty() || !customerResult.get().isOk()) {
            return Map.of();
        }
        List<?> accountIds = customerResult.get().recordOrThrow().getList("accounts");
        if (accountIds == null || accountIds.isEmpty()) {
            return Map.of();
        }

        List<Key> accountKeys = new ArrayList<>();
        for (Object accountId : accountIds) {
            accountKeys.add(accounts().id((String) accountId));
        }

        Map<String, Integer> counts = new HashMap<>();
        try (RecordStream stream = session.query(accountKeys).readingOnlyBins("owners").execute()) {
            stream.forEach(result -> {
                if (result.isOk()) {
                    List<?> owners = result.recordOrThrow().getList("owners");
                    if (owners != null) {
                        for (Object ownerId : owners) {
                            String id = (String) ownerId;
                            if (!customerId.equals(id)) {
                                counts.merge(id, 1, Integer::sum);
                            }
                        }
                    }
                }
            });
        }
        return counts;
    }

    /**
     * Gets the list of account ids related to a customer, or {@code null} if the customer does
     * not exist.
     */
    public List<String> getRelatedAccountIds(Session session, String customerId) {
        Optional<RecordResult> customerResult = session.query(customers().id(customerId)).execute().getFirst();
        if (customerResult.isEmpty() || !customerResult.get().isOk()) {
            return null;
        }
        List<?> accountIds = customerResult.get().recordOrThrow().getList("accounts");
        return accountIds == null ? null : accountIds.stream().map(id -> (String) id).collect(Collectors.toList());
    }

    /**
     * Removes the association between a customer and an account, inside a transaction: removes
     * the account id from the customer's {@code accounts} list, then the customer id from the
     * account's {@code owners} list.
     */
    public void removeAssociation(Session session, String customerId, String accountId) {
        session.doInTransaction(tx -> {
            RecordResult customerRemove = tx.upsert(customers().id(customerId))
                    .bin("accounts").onListValue(accountId).removeAnd().count()
                    .execute().getFirst().orElseThrow();
            boolean removedFromCustomer = customerRemove.recordOrThrow().getLong("accounts") > 0;
            if (!removedFromCustomer) {
                throw new IllegalStateException(String.format(
                        "Customer record for key '%s', should contain account id '%s' in its accounts list, but does not",
                        customerId, accountId));
            }

            RecordResult accountRemove = tx.upsert(accounts().id(accountId))
                    .bin("owners").onListValue(customerId).removeAnd().count()
                    .execute().getFirst().orElseThrow();
            boolean removedFromAccount = accountRemove.recordOrThrow().getLong("owners") > 0;
            if (!removedFromAccount) {
                throw new IllegalStateException(String.format(
                        "Account record for key '%s', should contain customer id '%s' in its owners list, but does not",
                        accountId, customerId));
            }
        });
    }

    public void displayRelatedCustomers(Map<String, Integer> relationships) {
        System.out.println(" Customer | Count");
        System.out.println("----------+------");
        for (Entry<String, Integer> entry : relationships.entrySet()) {
            System.out.printf("%9s | %,3d%n", entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void run(Session session) throws Exception {
        Map<String, Integer> result = getRelatedCustomers(session, "Cust-1");
        System.out.printf("%nFinding all the customers related to customer 'Cust-1' (%d):%n", result.size());
        displayRelatedCustomers(result);

        List<String> accounts = getRelatedAccountIds(session, "Cust-1");
        if (accounts != null && !accounts.isEmpty()) {
            System.out.printf("%nRemoving association between customer 'Cust-1' and account '%s'%n", accounts.get(0));
            removeAssociation(session, "Cust-1", accounts.get(0));

            result = getRelatedCustomers(session, "Cust-1");
            System.out.printf("%nRelationships after association was removed (%,d):%n", result.size());
            displayRelatedCustomers(result);
        }
    }
}
