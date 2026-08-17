package com.aerospike.examples.setup;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Session;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.setup.model.Account;
import com.aerospike.examples.setup.model.AccountMapper;

/**
 * SDK port of the legacy {@code SetupDemo} (see ../../java). The legacy version seeds data with
 * the Java Object Generator (Aerospike-independent, not wired up here yet - accounts are hand-built
 * instead) and the Java Object Mapper; here object mapping is done via the SDK's own
 * {@code RecordMapper}/{@code Cluster.setRecordMappingFactory} mechanism (see {@link
 * com.aerospike.examples.setup.model.AccountMapper}, registered in {@code UseCaseCookbookRunner}).
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

    private DataSet accounts() {
        return DataSet.of(System.getProperty("demo.namespace", "test"), "uccb_account");
    }

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
        DataSet accounts = accounts();
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
        DataSet accounts = accounts();
        AccountMapper mapper = (AccountMapper) session.getCluster().getRecordMappingFactory().getMapper(Account.class);

        System.out.println("Query first 100 accounts");
        try (RecordStream recordStream = session.query(accounts).limit(100).execute()) {
            recordStream.forEach(result -> {
                Record record = result.recordOrThrow();
                Account account = mapper.fromMap(record.bins, result.getKey(), record.generation);
                System.out.printf("Id: %s, Account Name: %s, Balance $%.02f, Date Opened: %s%n",
                        account.getId(),
                        account.getAccountName(),
                        account.getBalanceInCents() / 100.0,
                        account.getDateOpened());
            });
        }
    }
}