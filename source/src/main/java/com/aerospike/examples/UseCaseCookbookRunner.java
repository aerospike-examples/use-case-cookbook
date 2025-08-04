package com.aerospike.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.examples.gaming.Leaderboard;
import com.aerospike.examples.gaming.PlayerMatching;
import com.aerospike.examples.manytomany.ManyToManyRelationships;
import com.aerospike.examples.onetomany.OneToManyRelationships;
import com.aerospike.examples.setup.SetupDemo;
import com.aerospike.examples.timeseries.TimeSeriesDemo;
import com.aerospike.examples.timeseries.TimeSeriesLargeVarianceDemo;
import com.aerospike.mapper.tools.AeroMapper;

public class UseCaseCookbookRunner {
    
    // ANSI Color and Formatting Constants
    private static final String RESET = "\033[0m";
    private static final String BOLD = "\033[1m";
    private static final String RED = "\033[31m";
    private static final String GREEN = "\033[32m";
    private static final String YELLOW = "\033[33m";
    private static final String BLUE = "\033[34m";
    private static final String MAGENTA = "\033[35m";
    private static final String CYAN = "\033[36m";
    private static final String WHITE = "\033[37m";
    private static final String BLACK = "\033[30m";
    private static final String NORMAL = "";
    private static final String MEDIUM_GRAY = "\033[38;5;245m";
    private static final String REVERSE = "\033[7m";
    
    // Background colors
    private static final String BG_RED = "\033[41m";
    private static final String BG_GREEN = "\033[42m";
    private static final String BG_YELLOW = "\033[43m";
    private static final String BG_BLUE = "\033[44m";
    private static final String BG_MAGENTA = "\033[45m";
    private static final String BG_CYAN = "\033[46m";
    private static final String BG_WHITE = "\033[47m";
    
    // Combined colors for highlighting
    private static final String HIGHLIGHT = BOLD + BLUE; // Blue font, bold if possible
    
    // Menu color scheme - change these two constants to customize the menu colors
    private static final String COLOR1 = NORMAL;  // First alternating color
    private static final String COLOR2 = MEDIUM_GRAY; // Second alternating color
    
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
            new PlayerMatching(),
            new TimeSeriesDemo(),
            new TimeSeriesLargeVarianceDemo()
    );
    
    /** The Aerospike client to use for the demonstration */
    private final IAerospikeClient client;
    
    /** The mapper to use to perform repetitive tasks */
    private final AeroMapper mapper;
    
    /** Currently filtered use cases for search results */
    private List<UseCase> filteredUseCases;
    
    /** Current search term for highlighting */
    private String currentSearchTerm;
    
    /** Whether current search is using regex */
    private boolean isRegexSearch;
    
    public UseCaseCookbookRunner(IAerospikeClient client, AeroMapper mapper) {
        this.client = client;
        this.mapper = mapper;
        this.filteredUseCases = new ArrayList<>(useCases);
        this.currentSearchTerm = null;
        this.isRegexSearch = false;
    }
    
    /**
     * Search for use cases that match the given search term.
     * 
     * @param searchTerm the term to search for
     * @param useRegex whether to treat the search term as a regular expression
     */
    private void searchUseCases(String searchTerm, boolean useRegex) {
        this.currentSearchTerm = searchTerm;
        this.isRegexSearch = useRegex;
        
        if (searchTerm == null || searchTerm.trim().isEmpty()) {
            // Clear search - show all use cases
            this.filteredUseCases = new ArrayList<>(useCases);
            return;
        }
        
        Pattern pattern = null;
        if (useRegex) {
            try {
                pattern = Pattern.compile(searchTerm, Pattern.CASE_INSENSITIVE);
            } catch (Exception e) {
                // Invalid regex, treat as literal search
                pattern = Pattern.compile(Pattern.quote(searchTerm), Pattern.CASE_INSENSITIVE);
            }
        } else {
            pattern = Pattern.compile(Pattern.quote(searchTerm), Pattern.CASE_INSENSITIVE);
        }
        
        List<UseCase> results = new ArrayList<>();
        for (UseCase useCase : useCases) {
            if (matchesSearch(useCase, pattern)) {
                results.add(useCase);
            }
        }
        
        this.filteredUseCases = results;
    }
    
    /**
     * Check if a use case matches the search pattern.
     * 
     * @param useCase the use case to check
     * @param pattern the search pattern
     * @return true if the use case matches the search
     */
    private boolean matchesSearch(UseCase useCase, Pattern pattern) {
        String name = useCase.getName() != null ? useCase.getName() : "";
        String description = useCase.getDescription() != null ? useCase.getDescription() : "";
        String reference = useCase.getReference() != null ? useCase.getReference() : "";
        boolean matchesTag = Arrays.stream(useCase.getTags())
                .map(tag -> pattern.matcher(tag).find())
                .anyMatch(b -> b);
                
        
        return pattern.matcher(name).find() ||
               pattern.matcher(description).find() ||
               pattern.matcher(reference).find() ||
               matchesTag;
    }
    
    private String highlightTags(String text, String color) {
        String result = text;
        int startIndex = result.indexOf('`');
        while (startIndex >= 0) {
            int endIndex = result.indexOf('`', startIndex+1);
            if (endIndex >= 0) {
                result = result.substring(0, startIndex) + REVERSE + " " 
                        + result.substring(startIndex+1, endIndex) + " " + RESET + color
                        + result.substring(endIndex + 1);
            }
            startIndex = result.indexOf('`');
        }
        return result;
    }
    /**
     * Highlight search terms in text using ANSI color codes, preserving the given color.
     * 
     * @param text the text to highlight
     * @param preserveColor the color to preserve after highlighting
     * @return the text with highlighted search terms
     */
    private String highlightSearchTermsAndSetLineColor(String text, String preserveColor) {
        text = highlightTags(text, preserveColor);
        if (currentSearchTerm == null || currentSearchTerm.trim().isEmpty()) {
            return preserveColor + text + RESET;
        }
        
        Pattern pattern;
        if (isRegexSearch) {
            try {
                pattern = Pattern.compile(currentSearchTerm, Pattern.CASE_INSENSITIVE);
            } catch (Exception e) {
                // Invalid regex, treat as literal search
                pattern = Pattern.compile(Pattern.quote(currentSearchTerm), Pattern.CASE_INSENSITIVE);
            }
        } else {
            pattern = Pattern.compile(Pattern.quote(currentSearchTerm), Pattern.CASE_INSENSITIVE);
        }
        
        Matcher matcher = pattern.matcher(text);
        StringBuffer result = new StringBuffer();
        
        while (matcher.find()) {
            String replacement = HIGHLIGHT + matcher.group() + RESET + preserveColor; // Yellow background, black text, then restore color
            matcher.appendReplacement(result, replacement);
        }
        matcher.appendTail(result);
        
        return preserveColor + result.toString() + RESET;
    }
    
    /**
     * Determine the length of the longest use-case name in the filtered use cases.
     * 
     * @return The length of the longest use case name, or 0 if there are no use cases
     */
    public int getLongestUseCaseName() {
        int longest = 0;
        for (UseCase thisUseCase : filteredUseCases) {
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
        if (width > 0) {
            return String.format("%-" + width + "s", text);
        }
        else {
            return text;
        }
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

            // Apply word wrapping to each paragraph
            String[] words = paragraph.split(" ");
            
            if (words.length == 0) {
                // Add a blank line
                lines.add("");
            }
                else {
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
        }

        return lines;
    }
    
    /**
     * Form a full description of the use case and the reference link
     * @param uc - The use case to display
     * @return
     */
    public String formUseCaseText(UseCase uc) {
        String result = uc.getDescription() + "\n \nSee: " + uc.getReference();
        if (uc.getTags() != null && uc.getTags().length > 0) {
            result += "\nTags:";
            for (String thisTag : uc.getTags()) {
                result += " `" + thisTag + "`";
            }
        }
        return result;
    }
    

    /**
     * Displays the table of use cases in a formatted layout using ANSI colors.
     *
     * @param length - the total width of the table to display
     */
    public void showUseCaseDetails(int length, boolean summaryOnly) {
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
        
        if (nameWidth == 0) {
            // No matching use cases
            System.out.println("*** No matching use cases ***");
            return;
        }

        String indexHeader = padBoth("No.", indexWidth);
        String nameHeader = padBoth("Use Case", nameWidth);
        String descHeader = padBoth("Description", descWidth);

        String horizontalLine = repeat("-", length);
        System.out.println(horizontalLine);
        System.out.printf(BOLD + "| %s | %s | %s |" + RESET + "%n", indexHeader, nameHeader, descHeader);
        System.out.println(horizontalLine);

        for (int i = 0; i < filteredUseCases.size(); i++) {
            UseCase uc = filteredUseCases.get(i);
            String index = padLeft(String.valueOf(i + 1), indexWidth);
            String color = (i % 2 == 0) ? COLOR2 : COLOR1; // alternating colors
            String name = highlightSearchTermsAndSetLineColor(uc.getName(), color) + padRight("", nameWidth-uc.getName().length());
            List<String> wrappedDesc = wrapText(formUseCaseText(uc), descWidth);

            for (int j = 0; j < wrappedDesc.size(); j++) {
                String descLine = padRight(wrappedDesc.get(j), descWidth);
                String formattedLine;
                if (j == 0) {
                    formattedLine = String.format("| %s | %s | %s |", 
                            highlightSearchTermsAndSetLineColor(index, color),
                            name,
                            highlightSearchTermsAndSetLineColor(descLine, color));
                } else {
                    formattedLine = String.format("| %s | %s | %s |", 
                            padLeft("", indexWidth),
                            padRight("", nameWidth),
                            highlightSearchTermsAndSetLineColor(descLine, color));
                }
                
                System.out.println(formattedLine);

                if (summaryOnly) {
                    break;
                }
            }
            System.out.println(horizontalLine);
        }
    }
    
    public void showStartHeader() {
        System.out.println("Here are the use cases in this repository:");
        System.out.println("Use " + BOLD + "Command+Click" + RESET + " or " + BOLD + "Control+Click" + RESET + " to open hyperlinks in most modern terminals.");
    }
    
    /**
     * Runs the interactive use case menu.
     * Displays the table and prompts the user to select a use case by number.
     * Allows exiting by entering 0, "q", or "exit".
     * Supports search with "search term", "s term", or "/term" (regex).
     *
     * @param length the total width of the table to display
     */
    public void runMenu(int length) {
        try (Scanner scanner = new Scanner(System.in)) {

            boolean summary = false;
            while (true) {
                showStartHeader();
                showUseCaseDetails(length, summary);
                
                // Show search status if active
                if (currentSearchTerm != null && !currentSearchTerm.trim().isEmpty()) {
                    String searchType = isRegexSearch ? "regex" : "text";
                    System.out.println(CYAN + "Search active: '" + currentSearchTerm + "' (" + searchType + " search)" + RESET);
                }
                
                System.out.print("Enter a use case number (1 to " + filteredUseCases.size() + 
                               "), search command, summary, help, or exit: ");
                String input = scanner.nextLine().trim();
        
                // Handle exit methods
                if (input.equalsIgnoreCase("q") || input.equalsIgnoreCase("exit") || input.equals("0")) {
                    System.out.println("\n" + BOLD + "Exiting Use Case Menu. Goodbye!" + RESET + "\n");
                    break;
                }
                
                // Handle search commands
                if (input.startsWith("search ") || input.startsWith("s ")) {
                    String searchTerm = input.substring(input.indexOf(" ") + 1).trim();
                    if (searchTerm.isEmpty()) {
                        System.out.println("\n" + RED + "Error: Please provide a search term." + RESET + "\n");
                    } else {
                        searchUseCases(searchTerm, false);
                        System.out.println("\n" + CYAN + "Searching for: '" + searchTerm + "'" + RESET + "\n");
                    }
                    continue;
                }
                
                // Handle showing the summary of use cases
                if (input.equalsIgnoreCase("summary")) {
                    summary = true;
                    continue;
                }
                
                // Handle showing the full details of use cases
                if (input.equalsIgnoreCase("full") || input.equalsIgnoreCase("details")) {
                    summary = false;
                    continue;
                }
                
                // Handle regex search commands
                if (input.startsWith("/")) {
                    String searchTerm = input.substring(1).trim();
                    if (searchTerm.isEmpty()) {
                        System.out.println("\n" + RED + "Error: Please provide a regex pattern." + RESET + "\n");
                    } else {
                        searchUseCases(searchTerm, true);
                        System.out.println("\n" + CYAN + "Regex searching for: '" + searchTerm + "'" + RESET + "\n");
                    }
                    continue;
                }
                
                // Handle clear search
                if (input.equalsIgnoreCase("clear") || input.equalsIgnoreCase("c")) {
                    searchUseCases(null, false);
                    System.out.println("\n" + CYAN + "Search cleared." + RESET + "\n");
                    continue;
                }
                
                // Handle help command
                if (input.equalsIgnoreCase("help") || input.equalsIgnoreCase("h") || input.equals("?")) {
                    showHelp();
                    continue;
                }
        
                try {
                    int selection = Integer.parseInt(input);
                    if (selection >= 1 && selection <= filteredUseCases.size()) {
                        invokeUseCase(selection);
                        System.out.print("\nPress Enter to return to the menu...");
                        scanner.nextLine(); // wait for enter
                    } else {
                        System.out.println("\n" + RED + "Error: Number out of range. Please try again." + RESET + "\n");
                    }
                } catch (NumberFormatException e) {
                    System.out.println("\n" + RED + "Error: Invalid input. Please enter a number or search command." + RESET + "\n");
                    showHelp();
                }
            }
        }
    }

    /**
     * Invoke the selected use case.
     *
     * @param selection the 1-based index of the selected use case from the filtered list
     */
    public void invokeUseCase(int selection) {
        UseCase uc = filteredUseCases.get(selection - 1);
        System.out.println("\n" + BOLD + "Selected Use Case #" + selection + ": " + uc.getName() + RESET + "\n");
        
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
     * Display help information for search commands.
     */
    private void showHelp() {
        System.out.println("Help commands:");
        System.out.println("  help           - Show this help text");
        System.out.println("Display commands:");
        System.out.println("  summary        - Show only summary (one line) use case descriptions");
        System.out.println("  full / details - Show detailed use case descriptions");
        System.out.println("Search commands:");
        System.out.println("  search <term>  - Search for term in names, descriptions, and URLs");
        System.out.println("  s <term>       - Short form of search");
        System.out.println("  /<regex>       - Search using regular expression");
        System.out.println("  clear          - Clear search and show all use cases");
        System.out.println("Running a use case:");
        System.out.println("  <shown number> - Execute the use case with the specified number");
        System.out.println("Exiting the program:");
        System.out.println("  exit / quit    - Show this help text");
        System.out.println();
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
    
    /**
     * Show a warning to the user if the connected cluster does not have transactional support.
     */
    private static void showClusterWarning() {
        String namespace = System.getProperty("demo.namespace", "test");
        System.out.println(RED + BOLD + "*************");
        System.out.println("*  WARNING  *");
        System.out.println("*************" + RESET);
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
        WritePolicy wp = new WritePolicy();
        wp.sendKey = true;
        
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.setWritePolicyDefault(wp);
        
        try (IAerospikeClient client = new AerospikeClient(clientPolicy, hostList)) {
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
            UseCaseCookbookRunner runner = new UseCaseCookbookRunner(clientToUse, mapper);
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
