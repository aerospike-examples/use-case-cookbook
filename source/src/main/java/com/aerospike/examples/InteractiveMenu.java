package com.aerospike.examples;

import java.util.ArrayList;
import java.util.Arrays;
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
        
        int longestName = getLongestUseCaseName();
        int indexWidth = String.valueOf(filteredUseCases.size()).length();
        int nameWidth = Math.min(longestName + 2, 50);
        int descWidth = width - indexWidth - nameWidth - 8; // Account for formatting characters
        
        String indexHeader = padLeft("#", indexWidth);
        String nameHeader = padLeft("Name", nameWidth);
        String descHeader = padLeft(summary ? "Summary" : "Description", descWidth);
        
        String horizontalLine = repeat("-", width);
        System.out.println(horizontalLine);
        System.out.printf(AnsiColors.BOLD + "| %s | %s | %s |" + AnsiColors.RESET + "%n", indexHeader, nameHeader, descHeader);
        System.out.println(horizontalLine);
        
        for (int i = 0; i < filteredUseCases.size(); i++) {
            UseCase uc = filteredUseCases.get(i);
            String index = padLeft(String.valueOf(i + 1), indexWidth);
            String color = (i % 2 == 0) ? AnsiColors.COLOR2 : AnsiColors.COLOR1; // alternating colors
            String name = padRight(uc.getName(), nameWidth);
            List<String> wrappedDesc = wrapText(formUseCaseText(uc, summary), descWidth);
            
            for (int j = 0; j < wrappedDesc.size(); j++) {
                String descLine = padRight(wrappedDesc.get(j), descWidth);
                String formattedLine;
                if (j == 0) {
                    formattedLine = String.format("| %s | %s | %s |", index, name, descLine);
                } else {
                    formattedLine = String.format("| %s | %s | %s |", 
                            padLeft("", indexWidth),
                            padRight("", nameWidth),
                            descLine);
                }
                
                // Apply highlighting to the formatted line
                String highlightedLine = highlightTableLine(formattedLine, color, j == 0);
                System.out.println(color + highlightedLine + AnsiColors.RESET);
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
     * @param text The text to wrap
     * @param width The maximum width
     * @return List of wrapped lines
     */
    private List<String> wrapText(String text, int width) {
        List<String> lines = new ArrayList<>();
        String[] words = text.split("\\s+");
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
        
        return lines;
    }
    
    /**
     * Pad a string to the right with spaces.
     * @param text The text to pad
     * @param width The target width
     * @return The padded string
     */
    private String padRight(String text, int width) {
        return String.format("%-" + width + "s", text);
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
     * Highlight search terms in a table line.
     * @param line The line to highlight
     * @param color The base color for the line
     * @param isFirstLine Whether this is the first line of a use case
     * @return The highlighted line
     */
    private String highlightTableLine(String line, String color, boolean isFirstLine) {
        if (currentSearchTerm == null || currentSearchTerm.trim().isEmpty()) {
            return line;
        }
        
        // Find the positions of the actual text content (not padding)
        // The line format is: "| index | name | description |"
        String[] parts = line.split("\\|");
        if (parts.length < 4) {
            return line; // Not a valid table line
        }
        
        // Extract the content parts (trim whitespace)
        String indexPart = parts[1].trim();
        String namePart = parts[2].trim();
        String descPart = parts[3].trim();
        
        // Highlight each part separately
        String highlightedIndex = highlightSearchTerms(indexPart, color);
        String highlightedName = highlightSearchTerms(namePart, color);
        String highlightedDesc = highlightSearchTerms(descPart, color);
        
        // Apply bold formatting to name if it's the first line
        if (isFirstLine && !namePart.isEmpty()) {
            highlightedName = AnsiColors.BOLD + highlightedName + AnsiColors.RESET + color;
        }
        
        // Reconstruct the line with proper padding
        // Calculate visual length (without ANSI codes) for proper padding
        int indexVisualLength = indexPart.length();
        int nameVisualLength = namePart.length();
        int descVisualLength = descPart.length();
        
        // Add padding to match the original visual length
        String indexPadded = highlightedIndex + " ".repeat(Math.max(0, indexVisualLength - getVisualLength(highlightedIndex)));
        String namePadded = highlightedName + " ".repeat(Math.max(0, nameVisualLength - getVisualLength(highlightedName)));
        String descPadded = highlightedDesc + " ".repeat(Math.max(0, descVisualLength - getVisualLength(highlightedDesc)));
        
        return "| " + indexPadded + " | " + namePadded + " | " + descPadded + " |";
    }
    
    /**
     * Get the visual length of a string (ignoring ANSI codes).
     * @param text The text to measure
     * @return The visual length
     */
    private int getVisualLength(String text) {
        return text.replaceAll("\033\\[[0-9;]*[a-zA-Z]", "").length();
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
               pattern.matcher(useCase.getReference()).find();
    }
    
    /**
     * Invoke a use case by selection number.
     * @param selection The selection number (1-based)
     */
    private void invokeUseCase(int selection) {
        UseCase uc = filteredUseCases.get(selection - 1);
        executor.executeUseCase(uc, true); // true = interactive
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