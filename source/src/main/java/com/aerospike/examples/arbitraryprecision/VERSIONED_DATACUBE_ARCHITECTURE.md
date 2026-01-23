# Versioned DataCube Architecture

## Overview

The Versioned DataCube implementation provides a two-dimensional data structure stored in Aerospike with full version history. Both the cube metadata and individual data rows are versioned, allowing point-in-time queries to retrieve the entire cube as it existed at any moment in history.

## Core Concepts

### Two-Level Versioning

The architecture maintains versions at two levels:

1. **Cube Level**: Tracks metadata changes (dimensions, configuration) and coordinates the overall version number
2. **Row Level**: Tracks data changes in individual rows, with versions linked to cube versions

### Copy-on-Write Pattern

When changes are applied:
- Current records are copied to versioned keys before updates
- Only records affected by changes are versioned (efficient storage)
- All operations occur atomically within a transaction

### Efficient Storage Strategy

To optimize storage and performance:
- Multiple rows are stored in a single Aerospike record (configurable via `rowsPerRecord`)
- Target size: ~10KB per record for optimal network performance
- Historical records exclude the `versions` map (only in current records)

## Data Structures

### 1. Cube Record (Metadata)

The cube record stores metadata about the entire data cube.

#### Current Cube Record

**Key Format:** `userKey` (e.g., `"1"`)

**Bins:**
```java
{
    "version": 2,                           // Current version number (0, 1, 2, ...)
    "versions": {                           // Timestamp -> Version mapping
        1737654321000: 0,                   // At timestamp X, version 0 was active
        1737658921000: 1,                   // At timestamp Y, version 1 was active
        1737663521000: -1                   // Current version (special value -1)
    },
    "columns": 20,                          // Number of columns in the cube
    "rows": 15,                             // Number of rows in the cube
    "rowsPerRecord": 6                      // How many rows per Aerospike record
}
```

**Key Properties:**
- `version`: Integer counter, incremented with each change
- `versions`: Ordered map where keys are timestamps (ms), values are version numbers
- Special value `-1` in versions map indicates the current active version
- Contains cube configuration (dimensions, storage parameters)

#### Historical Cube Record

**Key Format:** `userKey:version` (e.g., `"1:0"`, `"1:1"`)

**Bins:**
```java
{
    "version": 0,                           // The historical version number
    // "versions" map is NOT present in historical records
    "columns": 20,
    "rows": 15,
    "rowsPerRecord": 6
}
```

**Key Properties:**
- Immutable snapshot of cube metadata at a specific version
- Does NOT include the `versions` map (only stored in current record)
- Allows reconstruction of cube state at any historical version

### 2. Row Record (Data Storage)

Row records store the actual data in the cube, with multiple rows per record for efficiency.

#### Current Row Record

**Key Format:** `userKey:startRow` (e.g., `"1:0"` for rows 0-5, `"1:6"` for rows 6-11)

The `startRow` is always a multiple of `rowsPerRecord`.

**Bins:**
```java
{
    "version": 2,                           // Current version of this row record
    "versions": {                           // Cube Version -> Row Version mapping
        0: 0,                               // When cube was v0, this row was v0
        1: 1,                               // When cube was v1, this row was v1
        2: -1                               // Current row version (special value -1)
    },
    "row-0": [3.14, 2.71, 1.41, ...],      // Data for row 0 (list of values)
    "row-1": [1.23, 4.56, 7.89, ...],      // Data for row 1
    "row-2": [9.87, 6.54, 3.21, ...],      // Data for row 2
    "row-3": [0.11, 0.22, 0.33, ...],      // Data for row 3
    "row-4": [5.55, 6.66, 7.77, ...],      // Data for row 4
    "row-5": [8.88, 9.99, 1.11, ...]       // Data for row 5
}
```

**Key Properties:**
- `version`: Integer counter for this row record, incremented when any contained row changes
- `versions`: **Important**: Keys are cube version numbers (not timestamps), values are row version numbers
- Special value `-1` indicates the current active row version
- Each `row-N` bin contains a list of values (one per column)
- Row bins are named `row-0`, `row-1`, etc. (local to the record, not global row IDs)

#### Historical Row Record

**Key Format:** `userKey:startRow:version` (e.g., `"1:0:0"`, `"1:0:1"`, `"1:6:0"`)

**Bins:**
```java
{
    "version": 0,                           // Historical version number
    // "versions" map is NOT present in historical records
    "row-0": [3.14, 2.71, 1.41, ...],
    "row-1": [1.23, 4.56, 7.89, ...],
    "row-2": [9.87, 6.54, 3.21, ...],
    "row-3": [0.11, 0.22, 0.33, ...],
    "row-4": [5.55, 6.66, 7.77, ...],
    "row-5": [8.88, 9.99, 1.11, ...]
}
```

**Key Properties:**
- Immutable snapshot of row data at a specific row version
- Does NOT include the `versions` map
- Multiple rows stored together (same as current record)

## Key Naming Conventions

### Cube Keys

| Key Type | Format | Example | Purpose |
|----------|--------|---------|---------|
| Current | `userKey` | `"1"` | Active cube metadata |
| Historical | `userKey:cubeVersion` | `"1:0"`, `"1:1"` | Historical cube metadata snapshots |

### Row Keys

| Key Type | Format | Example | Purpose |
|----------|--------|---------|---------|
| Current | `userKey:startRow` | `"1:0"`, `"1:6"`, `"1:12"` | Active row data (current version) |
| Historical | `userKey:startRow:rowVersion` | `"1:0:0"`, `"1:6:1"` | Historical row data snapshots |

**Key Calculation:**
```java
// For a cube with userKey="1" and rowsPerRecord=6
Row 0-5  -> Current: "1:0",  Historical: "1:0:0", "1:0:1", "1:0:2"
Row 6-11 -> Current: "1:6",  Historical: "1:6:0", "1:6:1", "1:6:2"
Row 12-17-> Current: "1:12", Historical: "1:12:0", "1:12:1", "1:12:2"

// startRow = (rowId / rowsPerRecord) * rowsPerRecord
```

## Versions Map Architecture

### Cube Versions Map

**Structure:** `Map<Long (timestamp), Integer (cubeVersion)>`

**Purpose:** Maps timestamps to cube versions for point-in-time queries

**Example:**
```java
{
    1737654321000: 0,     // Version 0 became active at this timestamp
    1737658921000: 1,     // Version 1 became active at this timestamp
    1737663521000: -1     // Current version (indicated by -1)
}
```

**Usage:**
- Find which cube version was active at any timestamp
- Special value `-1` marks the current version
- Ordered by key (timestamp) for efficient range queries
- Used for temporal queries: "Show me the cube as it was at 3pm yesterday"

### Row Versions Map

**Structure:** `Map<Long (cubeVersion), Integer (rowVersion)>`

**Purpose:** Links row versions to cube versions (NOT timestamps)

**Example:**
```java
{
    0: 0,     // When cube was version 0, this row was version 0
    1: 0,     // When cube was version 1, this row was still version 0 (no changes)
    2: 1,     // When cube was version 2, this row was updated to version 1
    3: -1     // At current cube version 3, this row is current (indicated by -1)
}
```

**Key Difference from Cube Versions Map:**
- **Keys are cube versions (Long), not timestamps**
- This links row versions to cube versions
- A row may not change with every cube version (storage optimization)
- Special value `-1` marks the current row version

**Why This Design?**
- Decouples row changes from cube changes
- A single cell update doesn't require versioning all rows
- Allows efficient reconstruction: "What was row 5 when cube was version 2?"

## Complete Example

Let's walk through a concrete example with a 15-row, 20-column cube with `rowsPerRecord=6`.

### Initial State (Version 0)

After initial population at timestamp `1737654321000`:

**Cube Record (`"1"`):**
```java
{
    "version": 0,
    "versions": {1737654321000: -1},  // Current version at creation time
    "columns": 20,
    "rows": 15,
    "rowsPerRecord": 6
}
```

**Row Records:**
- `"1:0"` - Current, contains rows 0-5, version=0, versions={0: -1}
- `"1:6"` - Current, contains rows 6-11, version=0, versions={0: -1}
- `"1:12"` - Current, contains rows 12-14, version=0, versions={0: -1}

### After First Update (Version 1)

Change cell at row 2, column 3 at timestamp `1737658921000`:

**Before Update:**
- Only row 2 is affected
- Row 2 is in record `"1:0"` (rows 0-5)

**After Update:**

**Cube Record (`"1"`)** - Updated:
```java
{
    "version": 1,                                      // Incremented
    "versions": {
        1737654321000: 0,                              // Old: Changed from -1 to 0
        1737658921000: -1                              // New: Current version
    },
    "columns": 20,
    "rows": 15,
    "rowsPerRecord": 6
}
```

**Cube Record (`"1:0"`)** - Created (Historical):
```java
{
    "version": 0,
    // No versions map
    "columns": 20,
    "rows": 15,
    "rowsPerRecord": 6
}
```

**Row Record (`"1:0"`)** - Updated:
```java
{
    "version": 1,                                      // Incremented
    "versions": {
        0: 0,                                          // Old: Changed from -1 to 0
        1: -1                                          // New: Current row version
    },
    "row-0": [...],                                    // Unchanged
    "row-1": [...],                                    // Unchanged
    "row-2": [...42.0...],                            // UPDATED cell at column 3
    "row-3": [...],                                    // Unchanged
    "row-4": [...],                                    // Unchanged
    "row-5": [...]                                     // Unchanged
}
```

**Row Record (`"1:0:0"`)** - Created (Historical):
```java
{
    "version": 0,
    // No versions map
    "row-0": [...],                                    // Original data
    "row-1": [...],
    "row-2": [...3.14...],                            // Original value at column 3
    "row-3": [...],
    "row-4": [...],
    "row-5": [...]
}
```

**Other Row Records** - Unchanged:
- `"1:6"` - Still version 0, versions={0: -1}, no historical copy needed
- `"1:12"` - Still version 0, versions={0: -1}, no historical copy needed

### After Second Update (Version 2)

Change cells at row 2, column 3 (again) and row 8, column 10 at timestamp `1737663521000`:

**Affected Records:**
- `"1:0"` (contains row 2)
- `"1:6"` (contains row 8, which is in rows 6-11)

**After Update:**

**Cube Record (`"1"`):**
```java
{
    "version": 2,
    "versions": {
        1737654321000: 0,
        1737658921000: 1,
        1737663521000: -1                              // Current
    },
    "columns": 20,
    "rows": 15,
    "rowsPerRecord": 6
}
```

**Cube Record (`"1:1"`)** - Created:
```java
{
    "version": 1,
    "columns": 20,
    "rows": 15,
    "rowsPerRecord": 6
}
```

**Row Record (`"1:0"`):**
```java
{
    "version": 2,
    "versions": {
        0: 0,
        1: 1,
        2: -1                                          // Current
    },
    "row-0": [...],
    "row-1": [...],
    "row-2": [...84.0...],                            // UPDATED again
    "row-3": [...],
    "row-4": [...],
    "row-5": [...]
}
```

**Row Record (`"1:0:1"`)** - Created:
```java
{
    "version": 1,
    "row-0": [...],
    "row-1": [...],
    "row-2": [...42.0...],                            // Previous value
    "row-3": [...],
    "row-4": [...],
    "row-5": [...]
}
```

**Row Record (`"1:6"`):**
```java
{
    "version": 1,                                      // First change to this record
    "versions": {
        0: 0,
        2: -1                                          // Note: Skipped version 1!
    },
    "row-0": [...],                                    // (Relative row IDs)
    "row-1": [...],
    "row-2": [...123.456...],                         // UPDATED (this is global row 8)
    "row-3": [...],
    "row-4": [...],
    "row-5": [...]
}
```

**Row Record (`"1:6:0"`)** - Created:
```java
{
    "version": 0,
    "row-0": [...],
    "row-1": [...],
    "row-2": [...],                                    // Original value
    "row-3": [...],
    "row-4": [...],
    "row-5": [...]
}
```

**Note:** Row record `"1:12"` is still version 0 with no changes, so no historical copy was needed.

## Point-in-Time Query Algorithm

To retrieve the cube as it existed at a specific timestamp:

### Step 1: Find Cube Version

Query the current cube record's versions map:
```java
MapOperation.getByKeyRelativeIndexRange("versions", 
    Value.get(timestamp+1), -1, 1, MapReturnType.KEY_VALUE)
```

This finds the cube version active at the given timestamp.

### Step 2: For Each Row Record

Using the cube version from Step 1:

1. Read the current row record
2. Check the `versions` map in the row record
3. Find which row version was active at that cube version
4. If the value is `-1`, use the current row data
5. If the value is a number, read the historical row record with that version

### Example Query

"What was the cube at timestamp 1737660000000?"

1. **Find Cube Version:**
   - Timestamp is between 1737658921000 and 1737663521000
   - Result: Cube version 1

2. **Read Row Record `"1:0"`:**
   - Current versions map: `{0: 0, 1: 1, 2: -1}`
   - At cube version 1, row version was 1
   - Read historical record `"1:0:1"`

3. **Read Row Record `"1:6"`:**
   - Current versions map: `{0: 0, 2: -1}`
   - At cube version 1, row version was still 0 (no entry for 1, use previous)
   - Read historical record `"1:6:0"`

4. **Read Row Record `"1:12"`:**
   - Current versions map: `{0: -1}`
   - At cube version 1, row version was still 0
   - Read historical record `"1:12:0"` (if exists) or current if no changes yet

## Storage Efficiency

### Space Complexity

For a cube with:
- R rows
- C columns
- V cube versions
- Average η% of rows changed per version

**Storage required:**
- Cube records: V + 1 (current + historical)
- Row records: (R / rowsPerRecord) × (1 + V × η) approximately

**Example:**
- 15 rows, 20 columns, 10 versions, 33% rows changed per version
- rowsPerRecord = 6
- Cube records: 11 (1 current + 10 historical)
- Row records: 3 current + ~10 historical = ~13 records
- Total: ~24 records

### Optimization Strategies

1. **Configurable rowsPerRecord**: Tune based on row size and change patterns
2. **Selective Versioning**: Only affected rows are versioned
3. **Version Compaction**: Periodically merge or purge old versions
4. **Lazy Deletion**: Old versions can be garbage collected based on retention policy

## Batch Operations

All versioning operations use batch operations for efficiency:

### Batch Read (Step 1)
- Read cube record (version, versions map)
- Read all affected row records (version, versions map)
- Single transaction, minimizes round trips

### Batch Write (Step 2)
- Create historical cube record copy
- Update current cube record
- For each affected row record:
  - Create historical row record copy
  - Update current row record with new data
- All writes in single transaction for atomicity

**Performance:**
- O(1) round trips regardless of number of changes
- Parallel execution with `maxConcurrentThreads = 0`
- Transaction ensures all-or-nothing consistency

## Best Practices

1. **Choose rowsPerRecord Wisely**: Balance between record size and update frequency
2. **Batch Multiple Changes**: Collect changes before calling `applyChanges()`
3. **Monitor Version Growth**: Implement retention policies for old versions
4. **Consider Change Patterns**: Hot rows should be in separate records
5. **Test Transaction Handling**: Ensure retry logic for concurrent updates

## Summary

The Versioned DataCube architecture provides:

✅ **Complete Version History** - Both metadata and data are versioned
✅ **Efficient Storage** - Only changed records are versioned
✅ **Point-in-Time Queries** - Reconstruct cube at any historical timestamp
✅ **Atomic Operations** - All changes in single transaction
✅ **Scalable Design** - Configurable storage and batching
✅ **Linked Versioning** - Row versions linked to cube versions for consistency

The two-level versioning with different map structures (cube uses timestamps, rows use cube versions) provides the perfect balance between query flexibility and storage efficiency.
