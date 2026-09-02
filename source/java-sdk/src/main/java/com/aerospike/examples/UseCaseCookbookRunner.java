package com.aerospike.examples;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Optional;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.mapper.tools.AeroMapper;

/**
 * Main entry point for the SDK port of the Use Case Cookbook.
 * <p/>
 * This is a port of the legacy {@code UseCaseCookbookRunner} (see ../../java): the named-use-case
 * path ({@code -uc}) and the interactive menu ({@link InteractiveMenu}, with its {@code search}/
 * {@code /regex} commands - see {@code README_SEARCH.md} in ../../java, unchanged here) are both
 * implemented. Cluster capability detection differs from the legacy version's full
 * strong-consistency check - see {@link #detectTransactionSupport}.
 * <p/>
 * Object mapping uses the annotation-driven {@code aerospike-sdk-mapper-java} library (JOM-style
 * {@code @AerospikeRecord}/{@code @AerospikeKey} annotations on each model class). Every model's
 * {@code namespace} is a {@code ${demo.namespace:test}} placeholder, which the mapper resolves
 * against the {@code -Ddemo.namespace} JVM system property (defaulting to {@code "test"}) - this
 * is why the {@code AeroMapper} built here is passed into every use case's {@code setup}/{@code
 * run}, so they can derive their {@code TypedDataSet}s via {@code
 * AeroMapper#getTypedDataSet(Class)} instead of hardcoding namespace/set a second time.
 */
public class UseCaseCookbookRunner {

    public static void main(String[] args) throws Exception {
        SdkConnector connector = new SdkConnector();
        Options options = connector.getOptions();
        options.addOption("uc", "useCaseName", true,
                "The name of the use case to run. Partial names are allowed. If omitted, an interactive menu is shown");
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
            System.exit(1);
            return;
        }

        boolean runOnly = cl.hasOption("runOnly");
        boolean seedOnly = cl.hasOption("seedOnly");
        if (runOnly && seedOnly) {
            System.out.println("Both 'runOnly' and 'seedOnly' cannot be specified.");
            usage(options);
            System.exit(1);
            return;
        }

        if (cl.hasOption("useCaseName") && cl.getOptionValue("useCaseName").isBlank()) {
            System.out.println("The use case name (-uc) cannot be blank.");
            usage(options);
            System.exit(1);
            return;
        }

        boolean success = true;
        try (Cluster cluster = connector.connect()) {
            // Bootstrap session used only to construct AeroMapper (no typed operations are
            // performed on it) - setRecordMappingFactory must happen before the real session used
            // for use case work is created, matching the existing detectTransactionSupport pattern
            // of a throwaway session for setup purposes.
            Session bootstrapSession = cluster.createSession(Behavior.DEFAULT);
            AeroMapper aeroMapper = new AeroMapper.Builder(bootstrapSession).build();
            cluster.setRecordMappingFactory(aeroMapper.asMappingFactory());

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

            if (cl.hasOption("useCaseName")) {
                success = executeUseCaseByName(cl.getOptionValue("useCaseName"), session, aeroMapper, seedOnly, runOnly);
            }
            else {
                runInteractiveMode(session, aeroMapper);
            }
        }
        if (!success) {
            System.exit(1);
        }
    }

    /** Runs the interactive menu ({@link InteractiveMenu}) when no {@code -uc} option is given. */
    private static void runInteractiveMode(Session session, AeroMapper mapper) throws Exception {
        InteractiveMenu menu = new InteractiveMenu(session, mapper);
        Terminal terminal = TerminalBuilder.terminal();
        int width = terminal.getWidth();
        if (width == 0) {
            width = 200;
        }
        menu.runMenu(width);
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

    /** @return {@code true} if the use case was found and completed without throwing. */
    private static boolean executeUseCaseByName(String useCaseName, Session session, AeroMapper mapper, boolean seedOnly, boolean runOnly) throws Exception {
        Optional<UseCase> useCaseOpt = UseCaseRegistry.findByName(useCaseName);
        if (useCaseOpt.isEmpty()) {
            List<UseCase> partialMatches = UseCaseRegistry.findAllByPartialName(useCaseName);
            if (partialMatches.size() == 1) {
                useCaseOpt = Optional.of(partialMatches.get(0));
            }
            else if (partialMatches.size() > 1) {
                System.err.println("Error: '" + useCaseName + "' matches more than one use case - be more specific:");
                partialMatches.forEach(uc -> System.err.println("   " + uc.getName()));
                return false;
            }
        }
        if (useCaseOpt.isEmpty()) {
            System.err.println("Error: Use case '" + useCaseName + "' not found.");
            listUseCases();
            return false;
        }

        UseCase useCase = useCaseOpt.get();
        System.out.println("\n" + AnsiColors.BOLD + "Executing Use Case: " + useCase.getName() + AnsiColors.RESET + "\n");
        try {
            if (!runOnly) {
                System.out.println("Setting up the data for the use case...");
                useCase.setup(session, mapper);
            }
            if (!seedOnly) {
                System.out.println("\nExecuting the use case...");
                useCase.run(session, mapper);
            }
            System.out.println(AnsiColors.GREEN + "\nUse case completed successfully!" + AnsiColors.RESET);
            return true;
        }
        catch (Exception e) {
            System.err.println(AnsiColors.RED + "An error occurred during execution of the use case:" + AnsiColors.RESET);
            System.err.println("   Message: " + e.getMessage());
            System.err.println("   Class: " + e.getClass().getName());
            e.printStackTrace();
            return false;
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