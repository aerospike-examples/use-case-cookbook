package com.aerospike.examples;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.DefaultRecordMappingFactory;
import com.aerospike.client.sdk.RecordMapper;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.examples.onetomany.model.Agent;
import com.aerospike.examples.onetomany.model.AgentMapper;
import com.aerospike.examples.onetomany.model.Listing;
import com.aerospike.examples.onetomany.model.ListingMapper;
import com.aerospike.examples.setup.model.Account;
import com.aerospike.examples.setup.model.AccountMapper;
import com.aerospike.examples.manytomany.model.Customer;
import com.aerospike.examples.manytomany.model.CustomerMapper;
import com.aerospike.examples.gaming.model.Player;
import com.aerospike.examples.gaming.model.PlayerMapper;
import com.aerospike.examples.transactionprocessing.model.Transaction;
import com.aerospike.examples.transactionprocessing.model.TransactionMapper;
import com.aerospike.examples.recordversioning.model.TradeBase;
import com.aerospike.examples.recordversioning.model.TradeBaseMapper;
import com.aerospike.examples.recordversioning.model.TradeStaticData;
import com.aerospike.examples.recordversioning.model.TradeStaticDataMapper;
import com.aerospike.examples.hotkeys.model.HotKeyProduct;
import com.aerospike.examples.hotkeys.model.HotKeyProductMapper;
import com.aerospike.examples.advancedexpressions.model.Car;
import com.aerospike.examples.advancedexpressions.model.CarMapper;

/**
 * Main entry point for the SDK port of the Use Case Cookbook.
 * <p/>
 * This is a pared-down port of the legacy {@code UseCaseCookbookRunner} (see ../../java) - only
 * the non-interactive, named-use-case path ({@code -uc}) is implemented so far. The interactive
 * menu, search, and cluster strong-consistency/transaction-shim detection have not been ported yet.
 * <p/>
 * Per Tim Faulkes' guidance on CLIENT-5234, object mapping uses the SDK's own built-in
 * {@code RecordMapper}/{@code Cluster.setRecordMappingFactory} mechanism rather than the external
 * {@code aerospike-sdk-mapper-java} project (which needs an unstable, hand-built {@code stage}
 * branch of the SDK to compile against). Every model's {@code RecordMapper} is registered here,
 * once, and shared by every use case - mirroring the single client+mapper pair the legacy
 * {@code UseCaseCookbookRunner} builds and hands to every use case.
 */
public class UseCaseCookbookRunner {

    private static Map<Class<?>, RecordMapper<?>> buildMappers() {
        Map<Class<?>, RecordMapper<?>> mappers = new HashMap<>();
        mappers.put(Account.class, new AccountMapper());
        mappers.put(Agent.class, new AgentMapper());
        mappers.put(Listing.class, new ListingMapper());
        mappers.put(Customer.class, new CustomerMapper());
        mappers.put(com.aerospike.examples.manytomany.model.Account.class, new com.aerospike.examples.manytomany.model.AccountMapper());
        mappers.put(Player.class, new PlayerMapper());
        mappers.put(Transaction.class, new TransactionMapper());
        mappers.put(com.aerospike.examples.transactionprocessing.model.Account.class, new com.aerospike.examples.transactionprocessing.model.AccountMapper());
        mappers.put(TradeBase.class, new TradeBaseMapper());
        mappers.put(TradeStaticData.class, new TradeStaticDataMapper());
        mappers.put(HotKeyProduct.class, new HotKeyProductMapper());
        mappers.put(Car.class, new CarMapper());
        return mappers;
    }

    public static void main(String[] args) throws Exception {
        SdkConnector connector = new SdkConnector();
        Options options = connector.getOptions();
        options.addOption("uc", "useCaseName", true, "The name of the use case to run. Partial names are allowed");
        options.addOption("l", "listUseCases", false, "Show a list of all the use cases");
        options.addOption("ro", "runOnly", false, "Only execute the use case, do not seed data for it");
        options.addOption("so", "seedOnly", false, "Only seed (generate) the data, do not execute the use case");
        options.addOption("?", "help", false, "Show this message");

        CommandLineParser parser = new DefaultParser();
        CommandLine cl = parser.parse(options, args, true);
        if (cl.hasOption("help")) {
            usage(options);
            return;
        }

        if (cl.hasOption("listUseCases")) {
            listUseCases();
            return;
        }

        String error = connector.validateConnectionsOptions(cl);
        if (error != null) {
            System.out.println(error);
            usage(options);
            return;
        }

        if (!cl.hasOption("useCaseName")) {
            System.out.println("No use case specified. Use -uc <name> to run one, or -l to list available use cases.");
            usage(options);
            return;
        }

        boolean runOnly = cl.hasOption("runOnly");
        boolean seedOnly = cl.hasOption("seedOnly");
        if (runOnly && seedOnly) {
            System.out.println("Both 'runOnly' and 'seedOnly' cannot be specified.");
            usage(options);
            return;
        }

        try (Cluster cluster = connector.connect()) {
            cluster.setRecordMappingFactory(new DefaultRecordMappingFactory(buildMappers()));

            boolean transactionsSupported = detectTransactionSupport(cluster);
            if (!transactionsSupported) {
                System.out.println(AnsiColors.YELLOW
                        + "Note: this cluster/namespace does not support multi-record transactions "
                        + "(requires Aerospike 8+ with a strong-consistency-enabled namespace). "
                        + "Transaction-based use cases will still run, but without the atomicity guarantee "
                        + "a real transaction would provide." + AnsiColors.RESET);
            }
            Session session = transactionsSupported
                    ? cluster.createSession(Behavior.DEFAULT)
                    : cluster.createSession(Behavior.DEFAULT, NonTransactionalCapableSession::new);

            executeUseCaseByName(cl.getOptionValue("useCaseName"), session, seedOnly, runOnly);
        }
    }

    /**
     * Probes whether the connected cluster/namespace supports multi-record transactions by
     * running a trivial write inside a transaction against a throwaway key. See {@link
     * NonTransactionalCapableSession} for what happens when it doesn't.
     */
    private static boolean detectTransactionSupport(Cluster cluster) {
        DataSet probe = DataSet.of(System.getProperty("demo.namespace", "test"), "uccb_txn_probe");
        Session session = cluster.createSession(Behavior.DEFAULT);
        try {
            session.doInTransaction(tx -> tx.upsert(probe.id("probe")).bin("x").setTo(1).execute());
            session.delete(probe.id("probe")).execute();
            return true;
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.UNSUPPORTED_FEATURE) {
                return false;
            }
            throw ae;
        }
    }

    private static void executeUseCaseByName(String useCaseName, Session session, boolean seedOnly, boolean runOnly) throws Exception {
        Optional<UseCase> useCaseOpt = UseCaseRegistry.findByName(useCaseName);
        if (useCaseOpt.isEmpty()) {
            useCaseOpt = UseCaseRegistry.findByPartialName(useCaseName);
        }
        if (useCaseOpt.isEmpty()) {
            System.err.println("Error: Use case '" + useCaseName + "' not found.");
            listUseCases();
            return;
        }

        UseCase useCase = useCaseOpt.get();
        System.out.println("\n" + AnsiColors.BOLD + "Executing Use Case: " + useCase.getName() + AnsiColors.RESET + "\n");
        try {
            if (!runOnly) {
                System.out.println("Setting up the data for the use case...");
                useCase.setup(session);
            }
            if (!seedOnly) {
                System.out.println("\nExecuting the use case...");
                useCase.run(session);
            }
            System.out.println(AnsiColors.GREEN + "\nUse case completed successfully!" + AnsiColors.RESET);
        }
        catch (Exception e) {
            System.err.println(AnsiColors.RED + "An error occurred during execution of the use case:" + AnsiColors.RESET);
            System.err.println("   Message: " + e.getMessage());
            System.err.println("   Class: " + e.getClass().getName());
            e.printStackTrace();
        }
    }

    private static void listUseCases() {
        System.out.println("Use cases:");
        for (UseCase useCase : UseCaseRegistry.getAllUseCases()) {
            System.out.println("   " + useCase.getName());
        }
    }

    private static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        String syntax = UseCaseCookbookRunner.class.getName() + " [<options>]";
        formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
        System.out.println(sw.toString());
    }
}