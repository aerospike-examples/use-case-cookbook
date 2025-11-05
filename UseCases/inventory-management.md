# Inventory Management System

## Overview

Managing inventory in real-time across multiple warehouses with concurrent operations is a common challenge in e-commerce, retail, and logistics. This use case demonstrates how to build a high-performance inventory management system using Aerospike that can:

- Handle thousands of concurrent stock updates per second
- Automatically detect and respond to low stock levels
- Maintain transaction history for audit trails
- Provide real-time inventory visibility
- Ensure data consistency with atomic operations

**Key Features:**
- Concurrent warehouse operations (sales, restocks, returns)
- Automatic reorder point detection
- Real-time monitoring and alerts
- Transaction logging
- Configurable simulation parameters

## Business Context

In a modern retail or e-commerce environment, inventory updates happen continuously:
- **Sales** - Customer purchases reduce stock levels
- **Restocking** - Warehouse receives shipments
- **Returns** - Customers return products
- **Adjustments** - Corrections for damaged or miscounted items

The system must handle these operations concurrently while maintaining accurate counts and triggering reorders when stock falls below defined thresholds. With Aerospike's atomic operations and sub-millisecond latency, you can achieve this at any scale.

## Data Model

### Product Entity

The `Product` entity represents an item in inventory with comprehensive attributes:

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `sku` | String (PK) | Stock Keeping Unit identifier (auto-generated via @GenExpression) | "SKU-000001" |
| `productName` | String | Human-readable product name | "Wireless Bluetooth Headphones" |
| `category` | String | Primary category | "Electronics" |
| `subcategory` | String | Secondary classification | "Audio" |
| `quantityOnHand` | int | Current stock level | 250 |
| `qtyReserved` | int | Reserved for pending orders (15-char limit) | 15 |
| `reorderPoint` | int | Trigger level for restock | 50 |
| `reorderQty` | int | Amount to order when restocking (15-char limit) | 200 |
| `unitCostCents` | long | Cost per unit (cents) | 4500 ($45.00) |
| `retailCents` | long | Selling price in cents (15-char limit) | 7999 ($79.99) |
| `weightGrams` | int | Weight in grams | 285 |
| `locations` | List<String> | Warehouse locations (15-char limit) | ["WH-EAST-A12", "WH-WEST-B04"] |
| `supplierName` | String | Supplier information | "TechSupply Inc." |
| `dateAdded` | Date | When added to catalog (auto-generated) | 2024-01-15 |
| `lastUpdate` | Date | Last inventory change (15-char limit) | 2024-03-20 14:35:22 |
| `active` | boolean | Currently in catalog (auto-generated) | true |

**Computed Fields:**
- `getAvailableQuantity()` - Returns `quantityOnHand - qtyReserved`
- `needsReorder()` - Returns true if available quantity is below reorder point

**Note on Field Names:** Several field names are shortened to comply with Aerospike's 15-character bin name limit. This is a hard limit in Aerospike, so always keep field names at or under 15 characters. Examples: `qtyReserved` instead of `quantityReserved`, `reorderQty` instead of `reorderQuantity`, `retailCents` instead of `retailPriceCents`, `locations` instead of `warehouseLocations`, `lastUpdate` instead of `lastStockUpdate`.

### Inventory Transaction Entity

The `InventoryTransaction` entity logs all inventory changes:

| Field | Type | Description |
|-------|------|-------------|
| `transactionId` | String (PK) | Unique transaction identifier (UUID) |
| `sku` | String | Product identifier |
| `type` | Enum | SALE, RESTOCK, ADJUSTMENT, RETURN, DAMAGE |
| `quantity` | int | Number of units (positive or negative) |
| `timestamp` | Date | When transaction occurred |
| `notes` | String | Additional information |

## Implementation Architecture

### Concurrent Operations Model

The system uses Aerospike's atomic operations to safely handle concurrent updates from multiple threads:

```java
// Atomic decrement for sales
client.operate(policy, key,
    Operation.add(new Bin("quantityOnHand", -quantity)),
    Operation.put(new Bin("lastUpdate", new Date().getTime()))
);
```

**Important:** Note that bin names like `lastUpdate` (10 chars) are kept short to comply with Aerospike's 15-character bin name limit.

This ensures that even with hundreds of concurrent sales, the inventory count remains accurate without race conditions.

### Async Library Usage

The demonstration uses three patterns from the `Async` library:

#### 1. `runFor()` - Time-bound Execution

Runs the entire simulation for a specified duration:

```java
Async.runFor(Duration.ofSeconds(RUNTIME_SECONDS.get()), (async) -> {
    // All operations run within this time window
});
```

#### 2. `periodic()` - Scheduled Tasks

Executes tasks at regular intervals:

```java
// Display status every 5 seconds
async.periodic(Duration.ofSeconds(MONITOR_INTERVAL_SECONDS.get()), () -> {
    displayInventoryStats(client);
});

// Check for reorders every 2 seconds
async.periodic(Duration.ofSeconds(2), () -> {
    checkAndReorderLowStock(client, mapper);
});
```

#### 3. `continuous()` - High-Throughput Operations

Runs operations as fast as possible with multiple threads:

```java
// Process sales continuously with multiple threads
async.continuous(NUM_WAREHOUSES.get(), () -> {
    processSale(client, mapper);
});
```

This creates `NUM_WAREHOUSES` threads, each processing sales as quickly as possible.

## Key Operations

### Processing a Sale

When a product is sold:

1. Select a random product
2. Atomically decrement the `quantityOnHand`
3. Update `lastUpdate` timestamp
4. Log the transaction

```java
private void processSale(IAerospikeClient client, AeroMapper mapper) {
    int productId = ThreadLocalRandom.current().nextInt(1, NUM_PRODUCTS.get() + 1);
    String sku = "SKU-" + String.format("%06d", productId);
    int quantity = ThreadLocalRandom.current().nextInt(1, 11);
    
    Key key = new Key(productNamespace, productSet, sku);
    
    // First check current quantity
    Record record = client.operate(policy, key,
        Operation.get("quantityOnHand")
    );
    
    if (record != null && record.getInt("quantityOnHand") >= quantity) {
        // Atomically update
        client.operate(policy, key,
            Operation.add(new Bin("quantityOnHand", -quantity)),
            Operation.put(new Bin("lastUpdate", new Date().getTime()))
        );
        
        salesCount.incrementAndGet();
        logTransaction(mapper, sku, TransactionType.SALE, quantity, "Sale processed");
    }
}
```

### Automatic Reordering

The system periodically checks products and automatically restocks those below the reorder point:

```java
private void checkAndReorderLowStock(IAerospikeClient client, AeroMapper mapper) {
    // Check random subset of products
    for (int i = 0; i < 10; i++) {
        String sku = getRandomSku();
        Product product = mapper.read(Product.class, sku);
        
        if (product != null && product.needsReorder()) {
            // Atomically add reorder quantity
            Key key = new Key(productNamespace, productSet, sku);
            client.operate(policy, key,
                Operation.add(new Bin("quantityOnHand", product.getReorderQty())),
                Operation.put(new Bin("lastUpdate", new Date().getTime()))
            );
            
            restockCount.incrementAndGet();
            logTransaction(mapper, sku, TransactionType.RESTOCK, 
                product.getReorderQty(), "Automatic reorder");
        }
    }
}
```

### Processing Returns

Returns increase inventory:

```java
private void processReturn(IAerospikeClient client, AeroMapper mapper) {
    String sku = getRandomSku();
    int quantity = ThreadLocalRandom.current().nextInt(1, 5);
    
    Key key = new Key(productNamespace, productSet, sku);
    client.operate(policy, key,
        Operation.add(new Bin("quantityOnHand", quantity)),
        Operation.put(new Bin("lastUpdate", new Date().getTime()))
    );
    
    logTransaction(mapper, sku, TransactionType.RETURN, quantity, "Customer return");
}
```

## Configuration with Parameters

Instead of hard-coded constants, the use case uses `Parameter<?>` objects that can be configured by users:

```java
private static final Parameter<Integer> NUM_PRODUCTS = 
    new Parameter<>("NUM_PRODUCTS", 1000, 
        "Number of products in the inventory system");

private static final Parameter<Integer> NUM_WAREHOUSES = 
    new Parameter<>("NUM_WAREHOUSES", 5, 
        "Number of concurrent warehouse operations (threads)");

private static final Parameter<Integer> RUNTIME_SECONDS = 
    new Parameter<>("RUNTIME_SECONDS", 30, 
        "How long to run the simulation in seconds");
```

Users can override these when running:

```bash
java -jar use-case-cookbook.jar --usecase="inventory" \
    --NUM_PRODUCTS=10000 \
    --NUM_WAREHOUSES=20 \
    --RUNTIME_SECONDS=60
```

## Data Generation with @GenMagic

The `Product` class uses `@GenMagic` and `@GenExpression` to automatically generate realistic test data:

```java
@Data
@NoArgsConstructor
@AllArgsConstructor
@GenMagic
@AerospikeRecord(namespace = "test", set = "uccb_product")
public class Product {
    @AerospikeKey
    @GenExpression("'SKU-' & @GenNumber(start = 1, end = 999999, format = '%06d')")
    private String sku;  // Generates "SKU-000001", "SKU-000002", etc.
    
    private String productName;  // Auto-generates realistic names
    private String category;     // Auto-generates categories
    
    @GenNumber(start = 0, end = 10000)
    private int quantityOnHand;  // Random between 0 and 10,000
    
    @GenNumber(start = 100, end = 1000000)
    private long unitCostCents;  // Random price
    
    @GenList(minItems = 1, maxItems = 5)
    private List<String> locations;  // 1-5 warehouse locations
    
    private Date dateAdded;  // @GenMagic auto-generates realistic dates
    private Date lastUpdate;  // Last stock update timestamp
    private boolean active;  // @GenMagic auto-generates boolean values
    
    // ... more fields
}
```

During setup, 1,000 products are generated with realistic data. Thanks to annotations, no manual field setting is needed:

```java
new Generator(Product.class)
    .generate(1, NUM_PRODUCTS.get(), 0, Product.class, product -> {
        // @GenExpression handles SKU formatting
        // @GenMagic populates all other fields
        // No manual field setting required!
        mapper.save(product);
    })
    .monitor();
```

The `.monitor()` call shows progress:
```
[1,234ms] 450 successful, 0 failed, 45.0% done
[2,156ms] 1,000 successful, 0 failed, 100.0% done
```

This approach significantly reduces boilerplate code and ensures consistency across all generated records.

## Running the Demo

### From the Interactive Menu

```bash
java -jar use-case-cookbook.jar
```

Select "Inventory Management System" from the menu.

### With Custom Parameters

```bash
java -jar use-case-cookbook.jar --usecase="inventory" \
    --NUM_PRODUCTS=5000 \
    --NUM_WAREHOUSES=10 \
    --RUNTIME_SECONDS=45
```

### Expected Output

```
================================================================================
INVENTORY MANAGEMENT SYSTEM SIMULATION
================================================================================
Running for 30 seconds with 5 concurrent warehouse operations
================================================================================

Sample Stats (first 100 products):
  Average Quantity: 4,523
  Low Stock Items: 8
  Out of Stock Items: 2

--------------------------------------------------------------------------------
Status Update (Sales: 1,247 | Restocks: 23 | Low Stock Alerts: 23)
--------------------------------------------------------------------------------
Sample Stats (first 100 products):
  Average Quantity: 4,156
  Low Stock Items: 12
  Out of Stock Items: 5

... (continues for 30 seconds) ...

================================================================================
SIMULATION COMPLETE - FINAL RESULTS
================================================================================
Total Sales: 6,432
Total Restocks: 187
Low Stock Alerts: 187
================================================================================
```

## Viewing Data with AQL

After running the demo, you can query the data using AQL:

### View Products

```sql
aql> SELECT * FROM test.uccb_product LIMIT 5

+-------------+--------------------------------+---------------+-------------+
| sku         | productName                    | quantityOnHand| retailCents |
+-------------+--------------------------------+---------------+-------------+
| "SKU-000001"| "Ergonomic Wireless Mouse"     | 425           | 2499          |
| "SKU-000002"| "USB-C Hub with 7 Ports"       | 189           | 4999          |
| "SKU-000003"| "Mechanical Gaming Keyboard"   | 76            | 12999         |
| "SKU-000004"| "27-inch LED Monitor"          | 342           | 29999         |
| "SKU-000005"| "Noise-Cancelling Headphones"  | 12            | 24999         |
+-------------+--------------------------------+---------------+---------------+
```

### View Product Details

```sql
aql> SELECT * FROM test.uccb_product WHERE PK='SKU-000001'

+-------------+--------------------------------+---------------+---------------+
| sku         | productName                    | category      | quantityOnHand|
+-------------+--------------------------------+---------------+---------------+
| "SKU-000001"| "Ergonomic Wireless Mouse"     | "Electronics" | 425           |
+-------------+--------------------------------+---------------+---------------+
```

### View Low Stock Items

To find items below reorder point, you'd read records and compare in your application:

```java
Product product = mapper.read(Product.class, "SKU-000001");
if (product.needsReorder()) {
    System.out.println("Reorder needed: " + product.getSku());
}
```

### View Transactions

```sql
aql> SELECT sku, type, quantity, timestamp FROM test.uccb_inventory_txn LIMIT 10

+-------------+----------+----------+-------------------+
| sku         | type     | quantity | timestamp         |
+-------------+----------+----------+-------------------+
| "SKU-000042"| "SALE"   | 3        | 1710953234567     |
| "SKU-000156"| "SALE"   | 7        | 1710953234589     |
| "SKU-000003"| "RESTOCK"| 500      | 1710953236012     |
| "SKU-000089"| "RETURN" | 2        | 1710953237123     |
| "SKU-000234"| "SALE"   | 5        | 1710953238456     |
+-------------+----------+----------+-------------------+
```

### Count Total Transactions

```sql
aql> SELECT COUNT(*) FROM test.uccb_inventory_txn
```

## Performance Considerations

### Throughput

With proper configuration, this pattern can handle:
- **Thousands of transactions per second** per node
- **Millions of products** in inventory
- **Sub-millisecond read latency** for stock checks
- **Consistent updates** even under heavy concurrent load

### Scaling Strategies

1. **Horizontal Scaling** - Add more Aerospike nodes to increase capacity
2. **Partitioning** - Data automatically distributed across cluster
3. **Replication** - Configure replication factor for high availability
4. **Rack Awareness** - Distribute replicas across failure domains

### Optimization Tips

1. **Batch Operations** - When checking multiple products, use batch reads:
   ```java
   Key[] keys = getProductKeys();
   Record[] records = client.get(batchPolicy, keys);
   ```

2. **Read Policies** - Adjust consistency levels based on requirements:
   ```java
   Policy policy = new Policy();
   policy.replica = Replica.MASTER_PROLES; // Read from master or replicas
   ```

3. **Write Policies** - Configure durability vs. performance:
   ```java
   WritePolicy policy = new WritePolicy();
   policy.commitLevel = CommitLevel.COMMIT_MASTER; // Fast writes
   ```

## Variations and Extensions

### 1. Multi-Warehouse Inventory

Track inventory separately per warehouse location:

```java
// Key: warehouseId:sku
Key key = new Key(namespace, set, warehouseId + ":" + sku);
```

### 2. Reserved Inventory

Implement cart reservation system:

```java
// Reserve items for shopping cart
client.operate(policy, key,
    Operation.add(new Bin("quantityOnHand", -quantity)),
    Operation.add(new Bin("qtyReserved", quantity))
);

// Complete purchase - move from reserved to sold
client.operate(policy, key,
    Operation.add(new Bin("qtyReserved", -quantity))
);
```

### 3. Expiring Reservations

Use TTL to automatically release reservations:

```java
WritePolicy policy = new WritePolicy();
policy.expiration = 900; // 15 minutes

// Create reservation record that auto-expires
client.put(policy, reservationKey, bins);
```

### 4. Stock Level Alerts

Implement notification system:

```java
if (product.getAvailableQuantity() < product.getReorderPoint()) {
    sendAlert("Low stock: " + product.getSku());
}
```

### 5. Price History Tracking

Store historical pricing in a list:

```java
Map<String, Object> priceChange = Map.of(
    "date", new Date(),
    "oldPrice", oldPrice,
    "newPrice", newPrice
);

client.operate(policy, key,
    ListOperation.append("priceHistory", Value.get(priceChange))
);
```

## Aerospike Features Demonstrated

This use case showcases:

- ✅ **Atomic Operations** - `Operation.add()` for thread-safe increments/decrements
- ✅ **Rich Data Types** - Maps, Lists, Strings, Numbers, Dates
- ✅ **Sub-millisecond Performance** - Fast reads and writes
- ✅ **Concurrent Operations** - Multiple threads updating safely
- ✅ **Java Object Mapper** - Automatic POJO ↔ Aerospike mapping
- ✅ **Data Generation** - @GenMagic for realistic test data
- ✅ **Flexible Schema** - Add fields without migration
- ✅ **High Availability** - Replication and rack awareness

## Source Code

Full implementation available at:
- **Main Class**: [InventoryManagementDemo.java](https://github.com/aerospike-examples/use-case-cookbook/blob/main/source/src/main/java/com/aerospike/examples/inventory/InventoryManagementDemo.java)
- **Product Model**: [Product.java](https://github.com/aerospike-examples/use-case-cookbook/blob/main/source/src/main/java/com/aerospike/examples/inventory/model/Product.java)
- **Transaction Model**: [InventoryTransaction.java](https://github.com/aerospike-examples/use-case-cookbook/blob/main/source/src/main/java/com/aerospike/examples/inventory/model/InventoryTransaction.java)

## Additional Resources

- [Aerospike Java Client Documentation](https://aerospike.com/docs/client/java/)
- [Aerospike Data Types](https://aerospike.com/docs/guide/data-types.html)
- [Aerospike Operations](https://aerospike.com/docs/guide/operations.html)
- [Best Practices for Data Modeling](https://aerospike.com/docs/guide/data-modeling.html)

---

**Next Steps**: Try modifying the parameters, implementing the variations above, or adapting this pattern to your own inventory management needs!


