package com.aerospike.examples;

/**
 * Centralized ANSI color and formatting constants.
 * This class provides all the color codes used throughout the application.
 */
public final class AnsiColors {
    
    // Prevent instantiation
    private AnsiColors() {}
    
    // Reset and formatting
    public static final String RESET = "\033[0m";
    public static final String BOLD = "\033[1m";
    public static final String REVERSE = "\033[7m";
    
    // Foreground colors
    public static final String BLACK = "\033[30m";
    public static final String RED = "\033[31m";
    public static final String GREEN = "\033[32m";
    public static final String YELLOW = "\033[33m";
    public static final String BLUE = "\033[34m";
    public static final String MAGENTA = "\033[35m";
    public static final String CYAN = "\033[36m";
    public static final String WHITE = "\033[37m";
    
    // Extended colors
    public static final String NORMAL = "";
    public static final String MEDIUM_GRAY = "\033[38;5;245m";
    
    // Background colors
    public static final String BG_RED = "\033[41m";
    public static final String BG_GREEN = "\033[42m";
    public static final String BG_YELLOW = "\033[43m";
    public static final String BG_BLUE = "\033[44m";
    public static final String BG_MAGENTA = "\033[45m";
    public static final String BG_CYAN = "\033[46m";
    public static final String BG_WHITE = "\033[47m";
    
    // Combined colors for highlighting
    public static final String HIGHLIGHT = BOLD + BLUE; // Blue font, bold if possible
    
    // Menu color scheme - change these two constants to customize the menu colors
    public static final String COLOR1 = NORMAL;  // First alternating color
    public static final String COLOR2 = MEDIUM_GRAY; // Second alternating color
} 