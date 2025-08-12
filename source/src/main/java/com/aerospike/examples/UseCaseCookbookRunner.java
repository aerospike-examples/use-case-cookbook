package com.aerospike.examples;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.Log;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.mapper.tools.AeroMapper;

/**
 * Main entry point for the Use Case Cookbook.
 * This class orchestrates the different execution modes (interactive menu vs batch).
 */
public class UseCaseCookbookRunner {
    

    
    @SuppressWarnings("serial")
    public static class MissingNamespaceException extends RuntimeException {}
    
    /**
     * Main function
     * Supports both interactive menu mode and batch command-line execution.
     */
    public static void main(String[] args) throws Exception {
        Log.setCallbackStandard();
        AerospikeConnector connector = new AerospikeConnector();
        Options options = connector.getOptions();

        // Add batch execution options
        BatchExecutor.addBatchOptions(options);
        args = BatchExecutor.correctParamNames(args);
        options.addOption("?",  "help", false, "Show this message");
        
        CommandLineParser parser = new DefaultParser();
        CommandLine cl = parser.parse(options, args, true);
        if (cl.hasOption("help")) {
            usage(options);
        }

        // Handle batch commands first
        if (BatchExecutor.handleBatchCommands(cl)) {
            return;
        }
        
        // Check if a specific use case was requested
        if (cl.hasOption("useCaseName")) {
            String useCaseName = cl.getOptionValue("useCaseName");
            
            WritePolicy wp = new WritePolicy();
            wp.sendKey = true;
            
            ClientPolicy clientPolicy = new ClientPolicy();
            clientPolicy.setWritePolicyDefault(wp);

            checkConnectionOptions(connector, cl, options);
            
            try (IAerospikeClient client = connector.connect(clientPolicy)) {
                IAerospikeClient clientToUse;
                if (!validateCluster(client)) {
                    showClusterWarning();
                    // Cluster does not have TEST as an SC namespace, or is < v8. Simulate transactions
                    clientToUse = AerospikeClientProxy.wrap(client);
                } else {
                    clientToUse = client;
                }
                AeroMapper mapper = new AeroMapper.Builder(clientToUse).build();
                
                // Execute the specified use case
                BatchExecutor.executeUseCaseByName(useCaseName, cl, clientToUse, mapper);
            } catch (MissingNamespaceException ignored) {
                System.out.printf("\nFATAL ERROR: This demo suite expects a namespace called '%s', but no nodes "
                        + "in the cluster at %s have this namespace", System.getProperty("demo.namespace", "test"), connector.getHosts());
            }
            return;
        }
        
        // Default to interactive menu mode
        runInteractiveMode(connector, cl, options);
    }
    
    /**
     * Run the interactive menu mode.
     */
    private static void runInteractiveMode(AerospikeConnector connector, CommandLine cl, Options options) throws Exception {
        WritePolicy wp = new WritePolicy();
        wp.sendKey = true;
        
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.setWritePolicyDefault(wp);

        checkConnectionOptions(connector, cl, options);
        
        try (IAerospikeClient client = connector.connect(clientPolicy)) {
            IAerospikeClient clientToUse;
            if (!validateCluster(client)) {
                showClusterWarning();
                // Cluster does not have TEST as an SC namespace, or is < v8. Simulate transactions
                clientToUse = AerospikeClientProxy.wrap(client);
            } else {
                clientToUse = client;
            }
            AeroMapper mapper = new AeroMapper.Builder(clientToUse).build();
            
            InteractiveMenu menu = new InteractiveMenu(clientToUse, mapper);
            Terminal terminal = TerminalBuilder.terminal();
            int width = terminal.getWidth();
            if (width == 0) {
                width = 200;
            }
            menu.runMenu(width);
        } catch (MissingNamespaceException ignored) {
            System.out.printf("\nFATAL ERROR: This demo suite expects a namespace called '%s', but no nodes "
                    + "in the cluster at %s have this namespace", System.getProperty("demo.namespace", "test"), connector.getHosts());
        }
    }
    
    private static void checkConnectionOptions(AerospikeConnector connector, CommandLine cl, Options options) {
        String error = connector.validateConnectionsOptions(cl);
        if (error != null) {
            System.out.println(error);
            usage(options);
        }
    }

    /**
     * Shows the usage
     * @param options
     */
    private static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        String syntax = UseCaseCookbookRunner.class.getName() + " [<options>]";
        formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
        System.out.println(sw.toString());
        System.out.println("If no use case is specified with the '-uc' option, an interactive menu will be shown.");
        System.exit(1);
    }

    /**
     * Show a warning to the user if the connected cluster does not have transactional support.
     */
    private static void showClusterWarning() {
        String namespace = System.getProperty("demo.namespace", "test");
        System.out.println(AnsiColors.RED + "*************");
        System.out.println("*  WARNING  *");
        System.out.println("*************" + AnsiColors.RESET);
        System.out.println("Some of the demonstrations in this program require transaction support. "
                + "Transactions require Aerospike version 8+ and the namespace ('"+namespace+"') must be "
                + "configured to support strong consistency (an enterprise edition enhancement). "
                + "Your cluster does not meet these requirements. The demonstrations will still "
                + "run, but transaction support will be disabled.\n");
    }
    
    /**
     * Validate that the cluster is set up correctly to run this application. There are two requirements:
     * <ul>
     * <li>A namespace called "test" exists
     * <li>The "test" namespace is a strong consistency namespace
     * </ul>
     * 
     * If the namespace exists but is not strongly consistent, the demo can continue and transactions will
     * be simulated. If the namespace does not exist 
     * @param client
     * @return
     */
    private static boolean validateCluster(IAerospikeClient client) {
        Node[] nodes = client.getNodes();
        boolean foundNamespace = false;
        for (Node thisNode : nodes) {
            String response = Info.request(thisNode, "build");
            int majorVersion = Integer.parseInt(response.substring(0, response.indexOf('.')));
            
            if (majorVersion < 8) {
                // Transactions were introduced in version 8
                return false;
            }
            
            String[] responses = Info.request(thisNode, "namespace/" + System.getProperty("demo.namespace", "test")).split(";");
            if (responses.length == 1) {
                // Namespace not found, just ignore
                continue;
            }
            foundNamespace = true;
            boolean found = false;
            for (String thisResponse : responses) {
                String key = thisResponse.substring(0, thisResponse.indexOf('='));
                if ("strong-consistency".equals(key)) {
                    String value = thisResponse.substring(thisResponse.indexOf('=')+1);
                    if (!"true".equalsIgnoreCase(value)) {
                        return false;
                    }
                    found = true;
                    break;
                }
            }
            if (!found) {
                // this should not happen
                return false;
            }
        }
        if (!foundNamespace) {
            throw new MissingNamespaceException();
        }
        return true;
    }
}
