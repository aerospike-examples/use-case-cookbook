# Contributing a New Use Case

This guide walks you through creating a new use case for the Use Case Cookbook. We'll use a complete, working example of an **Inventory Management System** that demonstrates all the key concepts and best practices.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Step-by-Step Guide](#step-by-step-guide)
4. [Complete Working Example](#complete-working-example)
5. [Testing Your Use Case](#testing-your-use-case)
6. [Checklist](#checklist)

## Overview

Adding a new use case involves five main steps:

1. **Create the data model** in an appropriate package
2. **Write the use case implementation** with `setup()` and `run()` methods
3. **Register the use case** in `UseCaseRegistry.java`
4. **Write comprehensive documentation** in a markdown file
5. **Update the README.md** with a brief description and link

## Prerequisites

- Java development environment set up
- Familiarity with Aerospike client operations
- Understanding of Project Lombok, Java Object Mapper, and Java Object Generator
- Access to a running Aerospike cluster

## Step-by-Step Guide

### Step 1: Create Your Data Model

Create your POJOs (Plain Old Java Objects) in a `model` subdirectory within your use case package. Use Lombok annotations for cleaner code and add `@GenMagic` for automatic data generation.

**Location:** `source/src/main/java/com/aerospike/examples/<your-package>/model/`

**Key annotations:**
- `@Data` - Generates getters, setters, toString, equals, and hashCode
- `@NoArgsConstructor` - Generates a no-argument constructor
- `@AllArgsConstructor` - Generates a constructor with all fields
- `@GenMagic` - Tells the generator to populate fields with realistic data
- `@AerospikeRecord` - Specifies namespace and set name
- `@AerospikeKey` - Marks the primary key field
- `@GenExpression` - Uses an expression to generate field values (e.g., `"'SKU-' & $Key"` for formatted IDs)
- `@GenNumber` - Specifies numeric ranges and formatting
- `@GenList` - Generates lists with specified cardinality

**Example:** See the complete `Product` and `InventoryTransaction` classes below.

**Pro Tip:** Use `@GenExpression` to format keys instead of manually setting them in your setup code. For example:
```java
@GenExpression("'SKU-' & PAD($Key, 6, '0')")
private String sku;
```
This automatically generates "SKU-000001", "SKU-000002", etc.

### Step 2: Implement the UseCase Interface

Create your main use case class that implements the `UseCase` interface.

**Location:** `source/src/main/java/com/aerospike/examples/<your-package>/`

**Required methods:**
- `String getName()` - Brief, descriptive name
- `String getDescription()` - Detailed description (searchable)
- `String getReference()` - URL to the full documentation markdown file
- `String[] getTags()` - Feature tags (optional but recommended)
- `Parameter<?>[] getParams()` - Configurable parameters (optional but recommended)
- `void setup(IAerospikeClient, AeroMapper)` - Data initialization
- `void run(IAerospikeClient, AeroMapper)` - Main demonstration logic

**Best practices:**
- Use `Parameter<?>` objects instead of hard-coded constants
- Use the `Async` library for concurrent operations
- Provide clear console output explaining what's happening
- Handle errors gracefully
- **IMPORTANT:** Create a private initialization method (e.g., `initializeNamespacesAndSets()`) that extracts namespace and set names from the mapper, and call it from BOTH `setup()` and `run()` methods. This ensures the use case works correctly even when run with the `--runOnly` flag which will skip the `setup()` step.

### Step 3: Register Your Use Case

Add your use case to the registry so it appears in the menu.

**File:** `source/src/main/java/com/aerospike/examples/UseCaseRegistry.java`

```java
private static final List<UseCase> USE_CASES = List.of(
        new SetupDemo(),
        new OneToManyRelationships(),
        // ... other use cases ...
        new YourNewUseCase(),  // Add your use case here
        new TopTransactionsAcrossDcs()
);
```

### Step 4: Create Documentation

Write comprehensive documentation explaining your use case.

**Location:** `UseCases/<your-use-case>.md`

**Documentation structure:**
- **Overview** - What problem does this solve?
- **Use Case Description** - Real-world scenarios
- **Data Model** - Explain your entities and relationships
- **Implementation Details** - How does your code work?
- **Code Walkthrough** - Explain key methods and operations
- **Performance Considerations** - Scaling, throughput, etc.
- **Running the Demo** - How to execute and what to expect
- **AQL Examples** - Show sample data and queries
- **Variations** - Alternative approaches
- **Source Code** - Link to the Java implementation

**Important:** Link from your Java code's `getReference()` method to this markdown file, and link from the markdown back to the source code.

### Step 5: Update README.md

Add a brief description and link to your documentation in the main README.

**File:** `README.md`

Add your entry in the "Use Cases" section:

```markdown
## Your Use Case Title
Brief one-sentence description. See [Your Use Case Title](UseCases/your-use-case.md)
```

## Complete Working Example

Here's a complete, working example of an Inventory Management System that demonstrates all best practices.

### Data Model: Product.java

```java
package com.aerospike.examples.inventory.model;

import java.util.Date;
import java.util.List;

import com.aerospike.generator.annotations.GenExpression;
import com.aerospike.generator.annotations.GenMagic;
import com.aerospike.generator.annotations.GenNumber;
import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a product in the inventory management system.
 * This model demonstrates a rich data structure with various field types
 * that work well with @GenMagic for automatic data generation.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@GenMagic
@AerospikeRecord(namespace = "test", set = "uccb_product")
public class Product {
    
    /**
     * Unique product identifier - serves as the primary key
     * Uses GenExpression to format the key as SKU-XXXXXX where X is zero-padded
     */
    @AerospikeKey
    @GenExpression("'SKU-' & PAD($Key, 6, '0')")
    private String sku;
    
    /**
     * Human-readable product name
     */
    private String productName;
    
    /**
     * Product category (e.g., "Electronics", "Clothing", "Food")
     */
    private String category;
    
    /**
     * Product subcategory for finer-grained classification
     */
    private String subcategory;
    
    /**
     * Current quantity in stock
     */
    private int quantityOnHand;
    
    /**
     * Quantity reserved for pending orders
     */
    private int qtyReserved;
    
    /**
     * Reorder point - when stock falls below this, trigger reorder
     */
    @GenNumber(start = 10, end = 100)
    private int reorderPoint;
    
    /**
     * Quantity to order when restocking
     */
    @GenNumber(start = 50, end = 1000)
    private int reorderQty;
    
    /**
     * Unit cost in cents
     */
    private long unitCostCents;
    
    /**
     * Retail price in cents
     */
    private long priceCents;
    
    /**
     * Weight in grams
     */
    private int weightGrams;
    
    /**
     * Warehouse locations where this product is stored
     */
    private List<String> locations;
    
    /**
     * Supplier information
     */
    private String supplierName;
    
    /**
     * Date when product was added to inventory
     * GenMagic automatically generates realistic dates
     */
    private Date dateAdded;
    
    /**
     * Date of last stock update
     * GenMagic automatically generates realistic dates
     */
    private Date lastUpdate;
    
    /**
     * Whether product is currently active in catalog
     */
    private boolean active;
    
    /**
     * Calculate available quantity (on hand minus reserved)
     */
    public int getAvailableQuantity() {
        return Math.max(0, quantityOnHand - qtyReserved);
    }
    
    /**
     * Check if product needs reordering
     */
    public boolean needsReorder() {
        return getAvailableQuantity() < reorderPoint;
    }
} 
```

### Data Model: InventoryTransaction.java

```java
package com.aerospike.examples.inventory.model;

import java.util.Date;

import com.aerospike.mapper.annotations.AerospikeKey;
import com.aerospike.mapper.annotations.AerospikeRecord;
import com.aerospike.generator.annotations.GenMagic;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a transaction affecting inventory levels.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@GenMagic
@AerospikeRecord(namespace = "test", set = "uccb_inventory_txn")
public class InventoryTransaction {
    
    public enum TransactionType {
        SALE,
        RESTOCK,
        ADJUSTMENT,
        RETURN,
        DAMAGE
    }
    
    @AerospikeKey
    private String transactionId;
    
    private String sku;
    private TransactionType type;
    private int quantity;
    private Date timestamp;
    private String notes;
}
```

### Use Case Implementation: InventoryManagementDemo.java

```java
package com.aerospike.examples.inventory;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.Operation;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.examples.Async;
import com.aerospike.examples.Parameter;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.inventory.model.Product;
import com.aerospike.examples.inventory.model.InventoryTransaction;
import com.aerospike.generator.Generator;
import com.aerospike.mapper.tools.AeroMapper;

/**
 * Inventory Management System Use Case
 * 
 * Demonstrates real-time inventory tracking with:
 * - Concurrent stock updates from multiple threads
 * - Automatic reorder point detection
 * - Transaction logging
 * - Stock level monitoring
 * 
 * This use case showcases:
 * - Using Parameters for configurability
 * - Async library with continuous, periodic, and runFor
 * - Rich data models with @GenMagic
 * - Atomic operations for inventory updates
 * - Real-time monitoring and reporting
 */
public class InventoryManagementDemo implements UseCase {
    
    // Parameters - configurable by users
    private static final Parameter<Integer> NUM_PRODUCTS = 
        new Parameter<>("NUM_PRODUCTS", 1000, 
            "Number of products in the inventory system");
    
    private static final Parameter<Integer> NUM_WAREHOUSES = 
        new Parameter<>("NUM_WAREHOUSES", 8, 
            "Number of concurrent warehouse operations (threads)");
    
    private static final Parameter<Integer> RUNTIME_SECONDS = 
        new Parameter<>("RUNTIME_SECONDS", 10, 
            "How long to run the simulation in seconds");
    
    private static final Parameter<Integer> MONITOR_INTERVAL_SECONDS = 
        new Parameter<>("MONITOR_INTERVAL_SECONDS", 5, 
            "How often to display inventory status");
    
    // Internal state
    private String productNamespace;
    private String productSet;
    private String transactionNamespace;
    private String transactionSet;
    
    // Counters for reporting
    private final AtomicInteger salesCount = new AtomicInteger(0);
    private final AtomicInteger restockCount = new AtomicInteger(0);
    private final AtomicInteger lowStockAlerts = new AtomicInteger(0);
    
    @Override
    public String getName() {
        return "Inventory Management System";
    }
    
    @Override
    public String getDescription() {
        return "Demonstrates real-time inventory management with concurrent updates, "
                + "automatic reorder detection, and transaction logging. Multiple warehouse "
                + "operations run concurrently, processing sales and restocking. The system "
                + "monitors stock levels and alerts when products fall below reorder points. "
                + "This use case showcases atomic operations, the Async library (continuous, "
                + "periodic, runFor), Parameters for configuration, and rich data models with @GenMagic.";
    }
    
    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/inventory-management.md";
    }
    
    @Override
    public String[] getTags() {
        return new String[] {
            "Parameters",
            "Async Library",
            "Expressions"
        };
    }
    
    @Override
    public Parameter<?>[] getParams() {
        return new Parameter<?>[] {
            NUM_PRODUCTS, 
            NUM_WAREHOUSES, 
            RUNTIME_SECONDS, 
            MONITOR_INTERVAL_SECONDS
        };
    }
    
    /**
     * Initialize namespace and set names from mapper.
     * This method should be called from both setup() and run() to ensure
     * the use case works correctly even when run with --runOnly flag.
     */
    private void initializeNamespacesAndSets(AeroMapper mapper) {
        productNamespace = mapper.getNamespace(Product.class);
        productSet = mapper.getSet(Product.class);
        transactionNamespace = mapper.getNamespace(InventoryTransaction.class);
        transactionSet = mapper.getSet(InventoryTransaction.class);
    }
    
    @Override
    public void setup(IAerospikeClient client, AeroMapper mapper) throws Exception {
        initializeNamespacesAndSets(mapper);
        
        // Clear existing data
        System.out.println("Clearing existing data...");
        client.truncate(null, productNamespace, productSet, null);
        client.truncate(null, transactionNamespace, transactionSet, null);
        
        // Generate sample products
        System.out.printf("Generating %,d products...%n", NUM_PRODUCTS.get());
        
        AtomicInteger productsCreated = new AtomicInteger(0);
        
        new Generator(Product.class)
            .generate(1, NUM_PRODUCTS.get(), 0, Product.class, product -> {
                // @GenExpression handles SKU formatting, @GenMagic handles dates and boolean
                // No manual field setting needed - generator does it all!
                productsCreated.incrementAndGet();
                
                // Save to database
                mapper.save(product);
            })
            .monitor();
        
        System.out.println("Setup complete!");
    }
    
    @Override
    public void run(IAerospikeClient client, AeroMapper mapper) throws Exception {
        initializeNamespacesAndSets(mapper);
        
        System.out.println("\n" + "=".repeat(80));
        System.out.println("INVENTORY MANAGEMENT SYSTEM SIMULATION");
        System.out.println("=".repeat(80));
        System.out.printf("Running for %d seconds with %d concurrent warehouse operations%n", 
            RUNTIME_SECONDS.get(), NUM_WAREHOUSES.get());
        System.out.println("=".repeat(80) + "\n");
        
        // Show initial state
        displayInventoryStats(client);
        
        // Run the simulation using Async library
        Async.runFor(Duration.ofSeconds(RUNTIME_SECONDS.get()), (async) -> {
            
            // PERIODIC: Display inventory status at regular intervals
            async.periodic(Duration.ofSeconds(MONITOR_INTERVAL_SECONDS.get()), () -> {
                System.out.println("\n" + "-".repeat(80));
                System.out.printf("Status Update (Sales: %,d | Restocks: %,d | Low Stock Alerts: %,d)%n",
                    salesCount.get(), restockCount.get(), lowStockAlerts.get());
                System.out.println("-".repeat(80));
                displayInventoryStats(client);
            });
            
            // CONTINUOUS: Process sales transactions as fast as possible with multiple threads
            async.continuous(NUM_WAREHOUSES.get(), () -> {
                processSale(client, mapper);
            });
            
            // PERIODIC: Check for low stock and reorder
            // Runs less frequently than sales processing
            async.periodic(Duration.ofSeconds(2), () -> {
                checkAndReorderLowStock(client, mapper);
            });
            
            // PERIODIC: Process returns occasionally
            async.periodic(Duration.ofSeconds(3), NUM_WAREHOUSES.get() / 2, () -> {
                processReturn(client, mapper);
            });
        });
        
        // Show final results
        System.out.println("\n" + "=".repeat(80));
        System.out.println("SIMULATION COMPLETE - FINAL RESULTS");
        System.out.println("=".repeat(80));
        System.out.printf("Total Sales: %,d%n", salesCount.get());
        System.out.printf("Total Restocks: %,d%n", restockCount.get());
        System.out.printf("Low Stock Alerts: %,d%n", lowStockAlerts.get());
        System.out.println("=".repeat(80) + "\n");
        
        displayInventoryStats(client);
        
        // Show some example products
        showExampleProducts(client, mapper);
    }
    
    /**
     * Process a sale transaction - reduces inventory
     */
    private void processSale(IAerospikeClient client, AeroMapper mapper) {
        try {
            // Pick a random product
            int productId = ThreadLocalRandom.current().nextInt(1, NUM_PRODUCTS.get() + 1);
            String sku = "SKU-" + String.format("%06d", productId);
            
            // Sale quantity between 1 and 10
            int quantity = ThreadLocalRandom.current().nextInt(1, 11);
            
            // Update inventory atomically
            Key key = new Key(productNamespace, productSet, sku);
            WritePolicy policy = client.copyWritePolicyDefault();
            
            
            // Update the quantity, but only if there is enough stock
            policy.filterExp = Exp.build(
                    Exp.ge(Exp.intBin("quantityOnHand"), Exp.val(quantity))
            );

            Record rec = client.operate(policy, key,
                Operation.add(new Bin("quantityOnHand", -quantity)),
                Operation.put(new Bin("lastUpdate", new Date().getTime()))
            );
            
            if (rec != null) {
                salesCount.incrementAndGet();
                
                // Log transaction
                logTransaction(mapper, sku, InventoryTransaction.TransactionType.SALE, 
                    quantity, "Sale processed");
            }
        } catch (Exception e) {
            // In a real system, handle errors appropriately
            // For demo purposes, we'll continue
        }
    }
    
    /**
     * Check inventory levels and restock if needed
     */
    private void checkAndReorderLowStock(IAerospikeClient client, AeroMapper mapper) {
        try {
            // Check a random subset of products
            for (int i = 0; i < 10; i++) {
                int productId = ThreadLocalRandom.current().nextInt(1, NUM_PRODUCTS.get() + 1);
                String sku = "SKU-" + String.format("%06d", productId);
                
                Product product = mapper.read(Product.class, sku);
                if (product != null && product.needsReorder()) {
                    // Restock!
                    Key key = new Key(productNamespace, productSet, sku);
                    WritePolicy policy = client.copyWritePolicyDefault();
                    
                    client.operate(policy, key,
                        Operation.add(new Bin("quantityOnHand", product.getReorderQty())),
                        Operation.put(new Bin("lastUpdate", new Date().getTime()))
                    );
                    
                    restockCount.incrementAndGet();
                    lowStockAlerts.incrementAndGet();
                    
                    // Log transaction
                    logTransaction(mapper, sku, InventoryTransaction.TransactionType.RESTOCK,
                        product.getReorderQty(), "Automatic reorder");
                }
            }
        } catch (Exception e) {
            // Handle errors
        }
    }
    
    /**
     * Process a return - increases inventory
     */
    private void processReturn(IAerospikeClient client, AeroMapper mapper) {
        try {
            int productId = ThreadLocalRandom.current().nextInt(1, NUM_PRODUCTS.get() + 1);
            String sku = "SKU-" + String.format("%06d", productId);
            int quantity = ThreadLocalRandom.current().nextInt(1, 5);
            
            Key key = new Key(productNamespace, productSet, sku);
            WritePolicy policy = client.copyWritePolicyDefault();
            
            client.operate(policy, key,
                Operation.add(new Bin("quantityOnHand", quantity)),
                Operation.put(new Bin("lastUpdate", new Date().getTime()))
            );
            
            logTransaction(mapper, sku, InventoryTransaction.TransactionType.RETURN,
                quantity, "Customer return");
        } catch (Exception e) {
            // Handle errors
        }
    }
    
    /**
     * Log an inventory transaction
     */
    private void logTransaction(AeroMapper mapper, String sku, 
            InventoryTransaction.TransactionType type, int quantity, String notes) {
        try {
            InventoryTransaction txn = new InventoryTransaction();
            txn.setTransactionId(UUID.randomUUID().toString());
            txn.setSku(sku);
            txn.setType(type);
            txn.setQuantity(quantity);
            txn.setTimestamp(new Date());
            txn.setNotes(notes);
            
            mapper.save(txn);
        } catch (Exception e) {
            // Handle errors
        }
    }
    
    /**
     * Display current inventory statistics
     */
    private void displayInventoryStats(IAerospikeClient client) {
        try {
            // Sample a few products to show stats
            int totalQty = 0;
            int lowStockCount = 0;
            int outOfStockCount = 0;
            int sampleSize = Math.min(100, NUM_PRODUCTS.get());
            
            for (int i = 1; i <= sampleSize; i++) {
                String sku = "SKU-" + String.format("%06d", i);
                Key key = new Key(productNamespace, productSet, sku);
                Record record = client.get(null, key);
                
                if (record != null) {
                    int qty = record.getInt("quantityOnHand");
                    int reorderPoint = record.getInt("reorderPoint");
                    totalQty += qty;
                    
                    if (qty == 0) {
                        outOfStockCount++;
                    } else if (qty < reorderPoint) {
                        lowStockCount++;
                    }
                }
            }
            
            System.out.printf("Sample Stats (first %d products):%n", sampleSize);
            System.out.printf("  Average Quantity: %,d%n", totalQty / sampleSize);
            System.out.printf("  Low Stock Items: %,d%n", lowStockCount);
            System.out.printf("  Out of Stock Items: %,d%n", outOfStockCount);
        } catch (Exception e) {
            System.err.println("Error displaying stats: " + e.getMessage());
        }
    }
    
    /**
     * Show some example products from the database
     */
    private void showExampleProducts(IAerospikeClient client, AeroMapper mapper) {
        System.out.println("\nExample Products from Database:");
        System.out.println("=".repeat(80));
        
        for (int i = 1; i <= 5; i++) {
            String sku = "SKU-" + String.format("%06d", i);
            Product product = mapper.read(Product.class, sku);
            
            if (product != null) {
                System.out.printf("SKU: %s%n", product.getSku());
                System.out.printf("  Name: %s%n", product.getProductName());
                System.out.printf("  Category: %s / %s%n", product.getCategory(), product.getSubcategory());
                System.out.printf("  Quantity: %d (Available: %d, Reserved: %d)%n",
                    product.getQuantityOnHand(), 
                    product.getAvailableQuantity(),
                    product.getQtyReserved());
                System.out.printf("  Price: $%.2f%n", product.getPriceCents() / 100.0);
                System.out.printf("  Needs Reorder: %s%n", product.needsReorder() ? "YES" : "No");
                System.out.println();
            }
        }
    }
}
```

### Register in UseCaseRegistry.java

```java
import com.aerospike.examples.inventory.InventoryManagementDemo;

private static final List<UseCase> USE_CASES = List.of(
        new SetupDemo(),
        new OneToManyRelationships(),
        new ManyToManyRelationships(),
        new Leaderboard(),
        new PlayerMatching(),
        new TimeSeriesDemo(),
        new TimeSeriesLargeVarianceDemo(),
        new InventoryManagementDemo(),  // <-- Add here
        new TopTransactionsAcrossDcs()
);
```

### Documentation: UseCases/inventory-management.md

Create a comprehensive markdown file documenting your use case. See `UseCases/timeseries-large-variance.md` for a good example of the structure and level of detail expected.

Key sections to include:
- Overview and business context
- Data model explanation with field descriptions
- Implementation walkthrough
- Code examples showing key operations
- Performance characteristics
- AQL query examples showing generated data
- Link back to source code

### Update README.md

Add to the use cases section:

```markdown
## Inventory Management System
Real-time inventory tracking with concurrent updates, automatic reorder detection, and transaction logging. Demonstrates the Async library, Parameters, and atomic operations. See [Inventory Management](UseCases/inventory-management.md)
```

## Testing Your Use Case

### 1. Build the Project

```bash
cd source
mvn clean package
```

### 2. Run Your Use Case

```bash
java -jar target/use-case-cookbook-*.jar
```

Select your use case from the menu.

### 3. Verify Data in AQL

```bash
aql
aql> select PK, category, lastUpdate, priceCents, qtyReserved, quantityOnHand, unitCostCents  from test.uccb_product limit 5
+--------------+------------+---------------+-------------+--------------+-----------------+----------------+
| PK           | category   | lastUpdate    | priceCents  | qtyReserved  | quantityOnHand  | unitCostCents  |
+--------------+------------+---------------+-------------+--------------+-----------------+----------------+
| "SKU-000790" | "Books"    | 1762321009068 | 7539        | 1171         | 1025            | 111            |
| "SKU-000254" | "Home"     | 1762321008888 | 6951        | 9756         | 4992            | 12             |
| "SKU-000094" | "Clothing" | 1762321008969 | 8526        | 378          | 9521            | 738            |
| "SKU-000742" | "Sports"   | 1762321009035 | 3293        | 9059         | 2514            | 87             |
| "SKU-000734" | "Health"   | 1762321008916 | 9784        | 194          | 8534            | 210            |
+--------------+------------+---------------+-------------+--------------+-----------------+----------------+
5 rows in set (0.012 secs)

OK


aql> SELECT * FROM test.uccb_inventory_txn LIMIT 10
+------------------+----------+--------------+---------------+----------------------------------------+--------+
| notes            | quantity | sku          | timestamp     | transactionId                          | type   |
+------------------+----------+--------------+---------------+----------------------------------------+--------+
| "Sale processed" | 7        | "SKU-000877" | 1762321008921 | "f0404a4d-8938-4a95-86c3-73a594452e95" | "SALE" |
| "Sale processed" | 1        | "SKU-000945" | 1762321007474 | "66792a3f-8d53-4921-921e-54df6c98a4a5" | "SALE" |
| "Sale processed" | 7        | "SKU-000893" | 1762321001304 | "ac474f00-a34c-40be-b686-dfdcda205ef0" | "SALE" |
| "Sale processed" | 6        | "SKU-000886" | 1762321003563 | "c2900297-2e94-43e3-80aa-3ae2594cc800" | "SALE" |
| "Sale processed" | 6        | "SKU-000983" | 1762321003309 | "8492f9fd-556f-4102-8b66-095149975db1" | "SALE" |
| "Sale processed" | 7        | "SKU-000765" | 1762321004213 | "6b677e60-0570-4942-8eb4-ef5ffc8cffd1" | "SALE" |
| "Sale processed" | 10       | "SKU-000461" | 1762321007782 | "5ab997c2-aa3d-47bf-9fed-57ad1d4fc958" | "SALE" |
| "Sale processed" | 1        | "SKU-000243" | 1762320999837 | "b9411ec7-8575-4cf4-b579-07978e6bce8f" | "SALE" |
| "Sale processed" | 8        | "SKU-000084" | 1762321008712 | "4acecc97-e41a-42d9-bb33-a121af17ecad" | "SALE" |
| "Sale processed" | 9        | "SKU-000712" | 1762321006576 | "d8860ccb-2f5d-4790-96a9-9c73ee6c691c" | "SALE" |
+------------------+----------+--------------+---------------+----------------------------------------+--------+
10 rows in set (0.050 secs)

OK

```

### 4. Test with Parameters

Run with custom parameter values:

```bash
java -jar target/use-case-cookbook-*.jar --usecase="inventory" --NUM_PRODUCTS=5000 --RUNTIME_SECONDS=60
```

## Checklist

Before submitting your use case, ensure you've completed:

- `[ ]` Created data model POJOs with Lombok and @GenMagic annotations
- `[ ]` Placed models in a `model/` subdirectory
- `[ ]` Verified all field names are 15 characters or less (Aerospike bin name limit)
- `[ ]` Used @GenExpression for formatted key fields instead of manual setting
- `[ ]` Implemented all required UseCase interface methods
- `[ ]` Created private initialization method for namespaces/sets
- `[ ]` Called initialization method from BOTH setup() and run()
- `[ ]` Used `Parameter<?>` objects for configuration the user should be able to control
- `[ ]` Use Async library (runFor, periodic, continuous) where needed
- `[ ]` Provided clear console output during execution
- `[ ]` Added to `UseCaseRegistry.java`
- `[ ]` Created comprehensive markdown documentation
- `[ ]` Linked `getReference()` to markdown file
- `[ ]` Linked markdown file back to source code
- `[ ]` Updated `README.md` with brief description
- `[ ]` Added appropriate tags via getTags()
- `[ ]` Included AQL query examples in documentation
- `[ ]` Tested the use case end-to-end
- `[ ]` Verified generated data in AQL
- `[ ]` Tested with different parameter values
- `[ ]` Tested with --runOnly flag to ensure run() works independently
- `[ ]` Code follows existing style and conventions

## Additional Best Practices

1. **Use meaningful variable names** - Code should be self-documenting
2. **Add JavaDoc comments** - Especially for public methods
3. **Handle errors gracefully** - Don't let exceptions crash the demo
4. **Provide progress indicators** - Use Generator's `.monitor()` method
5. **Show, don't just tell** - Display sample data, not just statistics
6. **Make it realistic** - Use business scenarios people can relate to
7. **Demonstrate Aerospike features** - Show off CDT operations, expressions, transactions, etc.
8. **Keep setup() fast** - Users want to run demos quickly
9. **Make run() interesting** - Show real-time updates, not just static queries
10. **Link everything together** - Documentation ↔ Code ↔ README

## Getting Help

If you need assistance:
- Review existing use cases for examples
- Check the Aerospike documentation: https://aerospike.com/docs/
- Ask questions in the GitHub issues

## Example File Structure

```
use-case-cookbook/
├── README.md (updated)
├── CONTRIBUTING.md (this file)
├── UseCases/
│   └── inventory-management.md (new)
└── source/src/main/java/com/aerospike/examples/
    ├── UseCaseRegistry.java (updated)
    └── inventory/
        ├── InventoryManagementDemo.java (new)
        └── model/
            ├── Product.java (new)
            └── InventoryTransaction.java (new)
```

---

**Thank you for contributing to the Use Case Cookbook!** Your examples help the Aerospike community learn best practices and design patterns.

