package com.aerospike.examples.setup;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.TypedDataSet;
import com.aerospike.client.sdk.TypedRecordStream;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.setup.model.Account;

/**
 * SDK port of the legacy {@code SetupDemo} (see ../../java). The legacy version seeds data with
 * the Java Object Generator (Aerospike-independent, not wired up here yet - accounts are hand-built
 * instead) and the Java Object Mapper; here object mapping is done via the SDK's own
 * {@code RecordMapper}/{@code Cluster.setRecordMappingFactory} mechanism (see {@link
 * com.aerospike.examples.setup.model.AccountMapper}, registered in {@code UseCaseCookbookRunner}).
 * <p/>
 * Uses {@link TypedDataSet} rather than a raw {@code DataSet} so reads decode straight to
 * {@link Account} via the registered mapper ({@link TypedRecordStream#forEachObject}), with no
 * manual {@code getMapper(...).fromMap(...)} step.
 */
public class SetupDemo implements UseCase {

    private static final int NUM_ACCOUNTS = 1_000;
    private static final String[] FIRST_NAMES = {"Estefana", "Arnoldo", "Jacquelyne", "Harris", "Ardelia"};
    private static final String[] LAST_NAMES = {"Ruecker", "MacGyver", "Willms", "Jones", "Renner"};

    @Override
    public String getName() {
        return "Demo setup";
    }

    @Override
    public String getDescription() {
        return "First application to make sure your environment is set up correctly. Inserts some Accounts and reads the data back";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/setup.md";
    }

    private final TypedDataSet<Account> accounts =
            TypedDataSet.of(System.getProperty("demo.namespace", "test"), "uccb_account", Account.class);

    private Account randomAccount() {
        String name = FIRST_NAMES[ThreadLocalRandom.current().nextInt(FIRST_NAMES.length)]
                + " " + LAST_NAMES[ThreadLocalRandom.current().nextInt(LAST_NAMES.length)] + "'s account";
        int balanceInCents = ThreadLocalRandom.current().nextInt(500, 2_000_000);
        long fiveYearsMs = 5L * 365 * 24 * 60 * 60 * 1000;
        Date dateOpened = new Date(System.currentTimeMillis() - ThreadLocalRandom.current().nextLong(0, fiveYearsMs));
        return new Account(UUID.randomUUID().toString(), name, balanceInCents, dateOpened);
    }

    @Override
    public void setup(Session session) throws Exception {
        session.truncate(accounts);

        System.out.printf("Generating %,d accounts...%n", NUM_ACCOUNTS);
        for (int i = 0; i < NUM_ACCOUNTS; i++) {
            Account account = randomAccount();
            session.upsert(accounts).object(account).execute();
        }
        System.out.println("Setup complete!");
    }

    @Override
    public void run(Session session) throws Exception {
        System.out.println("Query first 100 accounts");
        try (TypedRecordStream<Account> recordStream = session.query(accounts).limit(100).execute()) {
            recordStream.forEachObject(account -> System.out.printf(
                    "Id: %s, Account Name: %s, Balance $%.02f, Date Opened: %s%n",
                    account.getId(),
                    account.getAccountName(),
                    account.getBalanceInCents() / 100.0,
                    account.getDateOpened()));
        }
    }
}