# Search Functionality for Use Case Cookbook

The `UseCaseCookbookRunner` now includes powerful search functionality that allows users to find use cases by searching through their names, descriptions, and reference URLs.

## Search Commands

### Text Search
- `search <term>` - Search for a term in use case names, descriptions, and URLs
- `s <term>` - Short form of the search command

### Regular Expression Search
- `/<regex>` - Search using a regular expression pattern

### Other Commands
- `clear` or `c` - Clear the current search and show all use cases
- `help` or `h` or `?` - Show search help information

## Features

### Search Functionality
The search functionality filters use cases based on matches in names, descriptions, and reference URLs. When a search is active, only matching use cases are displayed in the menu. Matching terms are highlighted in yellow with black text, while maintaining proper table formatting and row colors.

### Search Status
The current search term and type (text or regex) are displayed above the menu when a search is active.

### Filtered Display
Only matching use cases are shown in the menu when a search is active.

## Examples

```
Enter a use case number (1 to 6), search command, or 0 to exit: search leaderboard
Searching for: 'leaderboard'

Search active: 'leaderboard' (text search)
```

```
Enter a use case number (1 to 6), search command, or 0 to exit: /leader.*
Regex searching for: 'leader.*'

Search active: 'leader.*' (regex search)
```

```
Enter a use case number (1 to 6), search command, or 0 to exit: clear
Search cleared.
```

## Implementation Details

The search functionality:
- Searches across use case names, descriptions, and reference URLs
- Supports both literal text search and regular expression search
- Highlights matching terms using ANSI color codes
- Maintains the original use case order in search results
- Provides clear feedback about search status and results
- Gracefully handles invalid regular expressions by falling back to literal search
- Uses centralized color constants for easy customization

### Color Customization

All ANSI color codes are defined as constants at the top of the `UseCaseCookbookRunner` class, making it easy to change the color scheme:

- `RESET`, `BOLD` - Basic formatting
- `RED`, `GREEN`, `YELLOW`, `BLUE`, `MAGENTA`, `CYAN`, `WHITE` - Foreground colors
- `BG_RED`, `BG_GREEN`, `BG_YELLOW`, etc. - Background colors
- `HIGHLIGHT` - Combined highlighting (yellow background, black text)

#### Quick Menu Color Changes

To change the alternating menu row colors, simply modify these two constants:

```java
private static final String COLOR1 = GREEN;  // First alternating color
private static final String COLOR2 = MAGENTA; // Second alternating color
```

For example, to use blue and cyan instead:
```java
private static final String COLOR1 = BLUE;   // First alternating color
private static final String COLOR2 = CYAN;   // Second alternating color
``` 