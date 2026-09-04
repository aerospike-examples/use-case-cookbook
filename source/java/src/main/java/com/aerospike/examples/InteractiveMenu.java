package com.aerospike.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.mapper.tools.AeroMapper;

/**
 * Handles the interactive menu system for use case selection and execution.
 * This class follows the Single Responsibility Principle by only handling menu interaction.
 */
public class InteractiveMenu {
    

    
    private final IAerospikeClient client;
    private final AeroMapper mapper;
    private final UseCaseExecutor executor;
    
    /** Currently filtered use cases for search results */
    private List<UseCase> filteredUseCases;
    
    /** Current search term for highlighting */
    private String currentSearchTerm;
    
    /** Whether current search is using regex */
    private boolean isRegexSearch;
    
    public InteractiveMenu(IAerospikeClient client, AeroMapper mapper) {
        this.client = client;
        this.mapper = mapper;
        this.executor = new UseCaseExecutor(client, mapper);
        this.filteredUseCases = new ArrayList<>(UseCaseRegistry.getAllUseCases());
    }
    
    /**
     * Run the interactive menu system.
     * @param width The terminal width for formatting
     */
    public void runMenu(int width) {
        boolean summary = false;
        boolean skipUseCaseDetails = false;
        
        while (true) {
            showStartHeader();
            if (!skipUseCaseDetails) {
                showUseCaseDetails(width, summary);
            }
            skipUseCaseDetails = false;
            
            // Show search status if active
            if (currentSearchTerm != null && !currentSearchTerm.trim().isEmpty()) {
                System.out.println(AnsiColors.YELLOW + "Search active: " + 
                    (isRegexSearch ? "regex '" : "'") + currentSearchTerm + "' " +
                    "(" + filteredUseCases.size() + " results)" + AnsiColors.RESET);
            }
            
            System.out.printf("Enter a number to run a use case, or enter a command (help for commands): ");
            Scanner scanner = new Scanner(System.in);
            String input = scanner.nextLine().trim();
            
            if (input.isEmpty()) {
                continue;
            }
            
            // Handle commands
            if (input.equalsIgnoreCase("help") || input.equalsIgnoreCase("h") || input.equals("?")) {
                showHelp();
                skipUseCaseDetails = true;
                continue;
            }
            
            if (input.equalsIgnoreCase("summary")) {
                summary = true;
                continue;
            }
            
            if (input.equalsIgnoreCase("full") || input.equalsIgnoreCase("details")) {
                summary = false;
                continue;
            }
            
            // Handle search commands
            if (input.startsWith("search ")) {
                String searchTerm = input.substring(7).trim();
                searchUseCases(searchTerm, false);
                continue;
            }
            
            if (input.startsWith("s ")) {
                String searchTerm = input.substring(2).trim();
                searchUseCases(searchTerm, false);
                continue;
            }
            
            if (input.startsWith("/")) {
                String searchTerm = input.substring(1).trim();
                searchUseCases(searchTerm, true);
                continue;
            }
            
            if (input.equalsIgnoreCase("clear") || input.equalsIgnoreCase("c")) {
                searchUseCases(null, false);
                continue;
            }
            
            if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit")) {
                System.out.println("Goodbye!");
                break;
            }
            
            // Handle use case selection
            try {
                int selection = Integer.parseInt(input);
                if (selection > 0 && selection <= filteredUseCases.size()) {
                    invokeUseCase(selection);
                } else {
                    System.out.println(AnsiColors.RED + "Invalid selection. Please enter a number between 1 and " + 
                        filteredUseCases.size() + "." + AnsiColors.RESET);
                }
            } catch (NumberFormatException e) {
                System.out.println(AnsiColors.RED + "Invalid input. Please enter a number or a command (type 'help' for available commands)." + AnsiColors.RESET);
            }
        }
    }
    
    /**
     * Show the start header.
     */
    private void showStartHeader() {
        System.out.println();
        System.out.println(AnsiColors.BOLD + "Aerospike Use Case Cookbook" + AnsiColors.RESET);
        System.out.println("==============================");
        System.out.println();
    }

    private String formatColors(String str, String color) {
        return color + highlightSearchTerms(str, color) + AnsiColors.RESET;
    }
    private String formatLine(String indexStr, String nameStr, String descStr, String format) {
        return String.format("| %s | %s | %s |", 
                formatColors(indexStr, format), 
                formatColors(nameStr, format), 
                formatColors(descStr, format));
    }
    
    private String formTagsString(String[] tags, String color) {
        StringBuffer sb = new StringBuffer().append("Tags: ");
        for (String tag : tags) {
            if (sb.length() > 0) {
                sb.append(' ');
            }
            sb.append(AnsiColors.REVERSE).append(tag).append(AnsiColors.RESET).append(color);
        }
        return sb.toString();
    }
    
    /**
     * Show use case details in a formatted table.
     * @param width The terminal width
     * @param summary Whether to show summary or full details
     */
    private void showUseCaseDetails(int width, boolean summary) {
        if (filteredUseCases.isEmpty()) {
            System.out.println(AnsiColors.RED + "No use cases found matching your search criteria." + AnsiColors.RESET);
            return;
        }
        
        final int columnSpaces = 6;
        final int columnChars = 4;
        int longestName = getLongestUseCaseName();
        int indexWidth = String.valueOf(filteredUseCases.size()).length();
        int nameWidth = Math.min(longestName, 50);
        int descWidth = width - indexWidth - nameWidth - columnChars - columnSpaces;
        
        String indexHeader = padCenter("#", indexWidth);
        String nameHeader = padCenter("Name", nameWidth);
        String descHeader = padCenter(summary ? "Summary" : "Description", descWidth);
        
        String horizontalLine = repeat("-", width);
        System.out.println(horizontalLine);
        System.out.println(formatLine(indexHeader, nameHeader, descHeader, AnsiColors.BOLD));
        
        System.out.println(horizontalLine);
        
        for (int i = 0; i < filteredUseCases.size(); i++) {
            UseCase uc = filteredUseCases.get(i);
            String index = padLeft(String.valueOf(i + 1), indexWidth);
            String color = (i % 2 == 0) ? AnsiColors.COLOR2 : AnsiColors.COLOR1; // alternating colors
            String name = padRight(uc.getName(), nameWidth);
            List<String> wrappedDesc = wrapText(formUseCaseText(uc, summary), descWidth);
            
            for (int j = 0; j < wrappedDesc.size(); j++) {
                String descLine = padRight(wrappedDesc.get(j), descWidth);
                if (j == 0) {
                    System.out.println(formatLine(index, name, descLine, color));
                } else {
                    System.out.println(formatLine(
                            padLeft("", indexWidth),
                            padRight("", nameWidth),
                            descLine, color));
                }
            }
            if (uc.getReference() != null) {
                System.out.println(formatLine( 
                        padLeft("", indexWidth),
                        padRight("", nameWidth),
                        padRight("", descWidth),
                        color));
                System.out.println(formatLine( 
                        padLeft("", indexWidth),
                        padRight("", nameWidth),
                        padRight("See: " + uc.getReference(), descWidth),
                        color));
            }
            if (uc.getTags() != null && uc.getTags().length > 0) {
                System.out.println(formatLine(
                        padLeft("", indexWidth),
                        padRight("", nameWidth),
                        padRight(formTagsString(uc.getTags(), color), descWidth),
                        color));
            }
            System.out.println(horizontalLine);
        }
    }
    
    /**
     * Get the longest use case name for formatting.
     * @return The length of the longest name
     */
    private int getLongestUseCaseName() {
        return filteredUseCases.stream()
                .mapToInt(uc -> uc.getName().length())
                .max()
                .orElse(0);
    }
    
    /**
     * Format use case text for display.
     * @param uc The use case
     * @param summary Whether to show summary or full description
     * @return The formatted text
     */
    private String formUseCaseText(UseCase uc, boolean summary) {
        if (summary) {
            return uc.getDescription().split("\\.")[0] + ".";
        } else {
            return uc.getDescription();
        }
    }
    
    /**
     * Wrap text to fit within a specified width.
     * Preserves explicit newlines in the text.
     * @param text The text to wrap
     * @param width The maximum width
     * @return List of wrapped lines
     */
    private List<String> wrapText(String text, int width) {
        List<String> lines = new ArrayList<>();
        
        // First split by explicit newlines to preserve them
        String[] paragraphs = text.split("\\n");
        
        for (String paragraph : paragraphs) {
            // Then wrap each paragraph independently
            String[] words = paragraph.split("\\s+");
            StringBuilder currentLine = new StringBuilder();
            
            for (String word : words) {
                if (currentLine.length() + word.length() + 1 <= width) {
                    if (currentLine.length() > 0) {
                        currentLine.append(" ");
                    }
                    currentLine.append(word);
                } else {
                    if (currentLine.length() > 0) {
                        lines.add(currentLine.toString());
                        currentLine = new StringBuilder(word);
                    } else {
                        // Word is longer than width, split it
                        lines.add(word.substring(0, width));
                        currentLine = new StringBuilder(word.substring(width));
                    }
                }
            }
            
            if (currentLine.length() > 0) {
                lines.add(currentLine.toString());
            }
        }
        
        return lines;
    }
    
    /**
     * Pad a string to the right with spaces.
     * @param text The text to pad
     * @param width The target width
     * @return The padded string
     */
    private String padRight(String text, int width) {
        int extraChars = 0;
        int startIndex = text.indexOf("\033");
        while (startIndex > 0) {
            int endIndex = text.indexOf('m', startIndex);
            extraChars += endIndex - startIndex + 1;
            startIndex = text.indexOf("\033", endIndex);
        }
        return String.format("%-" + (width+extraChars) + "s", text);
    }
    
    private String padCenter(String text, int width) {
        int strLength = text.length();
        int leftPadding = Math.max(0, (width - strLength)/2);
        int rightPadding = Math.max(0, width - leftPadding - strLength);
        return repeat(" ", leftPadding) + text + repeat(" ", rightPadding);
    }
    
    /**
     * Pad a string to the left with spaces.
     * @param text The text to pad
     * @param width The target width
     * @return The padded string
     */
    private String padLeft(String text, int width) {
        return String.format("%" + width + "s", text);
    }
    
    /**
     * Repeat a character a specified number of times.
     * @param ch The character to repeat
     * @param count The number of times to repeat
     * @return The repeated string
     */
    private String repeat(String ch, int count) {
        return ch.repeat(count);
    }
    
    /**
     * Highlight search terms in text.
     * @param text The text to highlight
     * @param preserveColor The color to preserve after highlighting
     * @return The highlighted text
     */
    private String highlightSearchTerms(String text, String preserveColor) {
        if (currentSearchTerm == null || currentSearchTerm.trim().isEmpty()) {
            return text;
        }
        
        try {
            Pattern pattern;
            if (isRegexSearch) {
                pattern = Pattern.compile(currentSearchTerm, Pattern.CASE_INSENSITIVE);
            } else {
                pattern = Pattern.compile(Pattern.quote(currentSearchTerm), Pattern.CASE_INSENSITIVE);
            }
            
            Matcher matcher = pattern.matcher(text);
            StringBuffer result = new StringBuffer();
            
            while (matcher.find()) {
                matcher.appendReplacement(result, AnsiColors.HIGHLIGHT + matcher.group() + AnsiColors.RESET + preserveColor);
            }
            matcher.appendTail(result);
            
            return result.toString();
        } catch (Exception e) {
            // If regex compilation fails, fall back to literal search
            return text.replaceAll("(?i)" + Pattern.quote(currentSearchTerm), 
                AnsiColors.HIGHLIGHT + "$0" + AnsiColors.RESET + preserveColor);
        }
    }
    
    /**
     * Search for use cases that match the given search term.
     * @param searchTerm The search term
     * @param useRegex Whether to use regex matching
     */
    private void searchUseCases(String searchTerm, boolean useRegex) {
        this.currentSearchTerm = searchTerm;
        this.isRegexSearch = useRegex;
        
        if (searchTerm == null || searchTerm.trim().isEmpty()) {
            // Clear search
            filteredUseCases = new ArrayList<>(UseCaseRegistry.getAllUseCases());
            return;
        }
        
        filteredUseCases = new ArrayList<>();
        Pattern pattern = null;
        
        try {
            if (useRegex) {
                pattern = Pattern.compile(searchTerm, Pattern.CASE_INSENSITIVE);
            } else {
                pattern = Pattern.compile(Pattern.quote(searchTerm), Pattern.CASE_INSENSITIVE);
            }
        } catch (Exception e) {
            // If regex compilation fails, fall back to literal search
            pattern = Pattern.compile(Pattern.quote(searchTerm), Pattern.CASE_INSENSITIVE);
        }
        
        for (UseCase uc : UseCaseRegistry.getAllUseCases()) {
            if (matchesSearch(uc, pattern)) {
                filteredUseCases.add(uc);
            }
        }
    }
    
    /**
     * Check if a use case matches the search pattern.
     * @param useCase The use case to check
     * @param pattern The search pattern
     * @return true if the use case matches
     */
    private boolean matchesSearch(UseCase useCase, Pattern pattern) {
        return pattern.matcher(useCase.getName()).find() ||
               pattern.matcher(useCase.getDescription()).find() ||
               pattern.matcher(useCase.getReference()).find() || 
               pattern.matcher(String.join(" ", useCase.getTags())).find();
    }
    
    /**
     * Invoke a use case by selection number.
     * @param selection The selection number (1-based)
     */
    private void invokeUseCase(int selection) {
        UseCase uc = filteredUseCases.get(selection - 1);
        executor.executeUseCase(uc, true, false, false); // true = interactive
    }
    
    /**
     * Display help information for commands.
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
        System.out.println("                  (If the use case has parameters, you'll be prompted to configure them)");
        System.out.println("Exiting the program:");
        System.out.println("  exit / quit    - Exit the program");
        System.out.println();
    }
} 