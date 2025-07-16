package com.aerospike.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.examples.gaming.Leaderboard;
import com.aerospike.examples.gaming.PlayerMatching;
import com.aerospike.examples.manytomany.ManyToManyRelationships;
import com.aerospike.examples.onetomany.OneToManyRelationships;
import com.aerospike.examples.setup.SetupDemo;
import com.aerospike.mapper.tools.AeroMapper;

public class Runner {
    
    @SuppressWarnings("serial")
    public static class MissingNamespaceException extends RuntimeException {}
    
    /**
     * A list of the use cases known about by this system. To add a new use case to the menu
     * and enable it to be executed, simply add it to this list. The order in the menu will 
     * be the same as the order in this list.
     */
    private final List<UseCase> useCases = List.of(
            new SetupDemo(),
            new OneToManyRelationships(),
            new ManyToManyRelationships(),
            new Leaderboard(),
            new PlayerMatching()
    );
    
    /** The Aerospike client to use for the demonstration */
    private final IAerospikeClient client;
    
    /** The mapper to use to perform repetitive tasks */
    private final AeroMapper mapper;
    
    public Runner(IAerospikeClient client, AeroMapper mapper) {
        this.client = client;
        this.mapper = mapper;
    }
    
    /**
     * Determine the length of the longest use-case name in all the use cases.
     * 
     * @return The length of the longest use case name, or 0 if there are no use cases
     */
    public int getLongestUseCaseName() {
        int longest = 0;
        for (UseCase thisUseCase : useCases) {
            if (thisUseCase.getName() != null && thisUseCase.getName().length() > longest) {
                longest = thisUseCase.getName().length();
            }
        }
        return longest;
    }
    
    /**
     * Pads a string to the right to fit a given width.
     *
     * @param text  the string to pad
     * @param width the target width
     * @return the right-padded string
     */
    private String padRight(String text, int width) {
        return String.format("%-" + width + "s", text);
    }

    /**
     * Pads a string to the left to fit a given width.
     *
     * @param text  the string to pad
     * @param width the target width
     * @return the left-padded string
     */
    private String padLeft(String text, int width) {
        return String.format("%" + width + "s", text);
    }

    /**
     * Pads a string on both sides to center it within a given width.
     *
     * @param text  the string to center
     * @param width the target width
     * @return the centered, space-padded string
     */
    private String padBoth(String text, int width) {
        int totalPadding = width - text.length();
        int left = totalPadding / 2;
        int right = totalPadding - left;
        return " ".repeat(left) + text + " ".repeat(right);
    }

    private String truncate(String text, int width) {
        return text.length() <= width ? text : text.substring(0, width - 1) + "…";
    }

    /**
     * Repeats a string a specified number of times.
     *
     * @param s     the string to repeat
     * @param count the number of times to repeat it
     * @return the resulting repeated string
     */
    private String repeat(String s, int count) {
        StringBuilder result = new StringBuilder(s.length() * count);
        for (int i = 0; i < count; i++) {
            result.append(s);
        }
        return result.toString();
    }

    /**
     * Wraps a long string to fit within a specified width, preserving word boundaries.
     * Also handles newlines in the text by treating them as forced line breaks.
     *
     * @param text  the text to wrap
     * @param width the maximum width per line
     * @return a list of wrapped lines
     */
    private List<String> wrapText(String text, int width) {
        List<String> lines = new ArrayList<>();
        
        // First split by newlines to handle forced line breaks
        String[] paragraphs = text.split("\n");
        
        for (String paragraph : paragraphs) {
            // Skip empty paragraphs (consecutive newlines)
            if (paragraph.trim().isEmpty()) {
                continue;
            }
            
            // Apply word wrapping to each paragraph
            String[] words = paragraph.split(" ");
            StringBuilder line = new StringBuilder();

            for (String word : words) {
                if (line.length() + word.length() + 1 > width) {
                    lines.add(line.toString());
                    line = new StringBuilder();
                }
                if (line.length() > 0) line.append(" ");
                line.append(word);
            }
            if (line.length() > 0) {
                lines.add(line.toString());
            }
        }

        return lines;
    }
    
    /**
     * Displays the table of use cases in a formatted layout using ANSI colors.
     *
     * @param length the total width of the table to display
     */
    public void showUseCaseDetails(int length) {
        if (length < 20) {
            System.out.println("Length too short to format table.");
            return;
        }

        int nameWidth = getLongestUseCaseName();
        int indexWidth = 5; // enough for 4 digits and space
        int totalPadding = 3 * 2; // 2 spaces per column
        int numSeparators = 4; // left edge + 3 columns
        int descWidth = length - (indexWidth + nameWidth + totalPadding + numSeparators);

        if (descWidth < 10) {
            System.out.println("Length too small to fit content properly.");
            return;
        }

        String indexHeader = padBoth("No.", indexWidth);
        String nameHeader = padBoth("Use Case", nameWidth);
        String descHeader = padBoth("Description", descWidth);

        String horizontalLine = repeat("-", length);
        System.out.println(horizontalLine);
        System.out.printf("\033[1m| %s | %s | %s |\033[0m%n", indexHeader, nameHeader, descHeader);
        System.out.println(horizontalLine);

        for (int i = 0; i < useCases.size(); i++) {
            UseCase uc = useCases.get(i);
            String index = padLeft(String.valueOf(i + 1), indexWidth);
            String name = padRight(truncate(uc.getName(), nameWidth), nameWidth);
            List<String> wrappedDesc = wrapText(uc.getDescription(), descWidth);

            String color = (i % 2 == 0) ? "\033[34m" : "\033[32m"; // blue and green alternating

            for (int j = 0; j < wrappedDesc.size(); j++) {
                String descLine = padRight(wrappedDesc.get(j), descWidth);
                if (j == 0) {
                    System.out.printf("%s| %s | %s | %s |\033[0m%n", color, index, name, descLine);
                } else {
                    System.out.printf("%s| %s | %s | %s |\033[0m%n",
                            color,
                            padLeft("", indexWidth),
                            padRight("", nameWidth),
                            descLine);
                }
            }
        }

        System.out.println(horizontalLine);
    }
    
    /**
     * Runs the interactive use case menu.
     * Displays the table and prompts the user to select a use case by number.
     * Allows exiting by entering 0, "q", or "exit".
     *
     * @param length the total width of the table to display
     */
    public void runMenu(int length) {
        try (Scanner scanner = new Scanner(System.in)) {

            while (true) {
                showUseCaseDetails(length);
                System.out.print("Enter a use case number (1 to " + useCases.size() + ", or 0 to exit): ");
                String input = scanner.nextLine().trim();
        
                // Handle exit methods
                if (input.equalsIgnoreCase("q") || input.equalsIgnoreCase("exit") || input.equals("0")) {
                    System.out.println("\n\033[1mExiting Use Case Menu. Goodbye!\033[0m\n");
                    break;
                }
        
                try {
                    int selection = Integer.parseInt(input);
                    if (selection >= 1 && selection <= useCases.size()) {
                        invokeUseCase(selection);
                        System.out.print("\nPress Enter to return to the menu...");
                        scanner.nextLine(); // wait for enter
                    } else {
                        System.out.println("\n\033[31mError: Number out of range. Please try again.\033[0m\n");
                    }
                } catch (NumberFormatException e) {
                    System.out.println("\n\033[31mError: Invalid input. Please enter a number.\033[0m\n");
                }
            }
        }
    }

    /**
     * Invoke the selected use case.
     *
     * @param selection the 1-based index of the selected use case
     */
    public void invokeUseCase(int selection) {
        UseCase uc = useCases.get(selection - 1);
        System.out.println("\n\033[1mSelected Use Case #" + selection + ": " + uc.getName() + "\033[0m\n");
        
        try {
            System.out.println("\nSetting up the data for the use case...");
            uc.setup(client, mapper);
            
            System.out.println("\nExecuting the use case...");
            uc.run(client, mapper);
        }
        catch (Exception e) {
            System.err.println("An error occurred during execution of the use case. The error detais are:");
            System.err.println("   Message: " + e.getMessage());
            System.err.println("   Class: " + e.getClass().getName());
            System.err.println("   Stack trace:");
            e.printStackTrace();
            
        }
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
    
    private static void showClusterWarning() {
        String namespace = System.getProperty("demo.namespace", "test");
        System.out.println("*************");
        System.out.println("*  WARNING  *");
        System.out.println("*************");
        System.out.println("Some of the demonstrations in this program require transaction support. "
                + "Transactions require Aerospike version 8+ and the namespace ('"+namespace+"') must be "
                + "configured to support strong consistency (an enterprise edition enhancement). "
                + "Your cluster does not meet these requirements. The demonstrations will still "
                + "run, but transaction support will be disabled.\n");
    }
    
    /**
     * Main function
     * At the moment only one argument is accepted, the host and port of the cluster. By default this is
     * "localhost:3000". Currently there is no way to attach to secured clusters.
     */
    public static void main(String[] args) throws IOException {
        String hosts = "localhost:3000";
        if (args.length == 1) {
            hosts = args[0];
        }
        Host[] hostList = Host.parseHosts(hosts, 3000);
        try (IAerospikeClient client = new AerospikeClient(null, hostList)) {
            IAerospikeClient clientToUse;
            if (!validateCluster(client)) {
                showClusterWarning();
                // Cluster does not have TEST as an SC namespace, or is < v8. Simulate transaactions
                clientToUse = AerospikeClientProxy.wrap(client);
            }
            else {
                clientToUse = client;
            }
            AeroMapper mapper = new AeroMapper.Builder(clientToUse).build();
            Runner runner = new Runner(clientToUse, mapper);
            Terminal terminal = TerminalBuilder.terminal();
            int width = terminal.getWidth();
            if (width == 0) {
                width = 200;
            }
            runner.runMenu(width);
        }
        catch (MissingNamespaceException ignored) {
            System.out.printf("\nFATAL ERROR: This demo suite expects a namespace called '%s', but no nodes "
                    + "in the cluster at %s have this namespace", System.getProperty("demo.namespace", "test"), hosts);
        }
    }
}
