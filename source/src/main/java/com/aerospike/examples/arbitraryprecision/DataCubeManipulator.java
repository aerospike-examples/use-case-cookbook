package com.aerospike.examples.arbitraryprecision;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Exp.Type;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.examples.AerospikeClientProxy;
import com.aerospike.examples.Utils;
import com.aerospike.examples.arbitraryprecision.model.CellConverter;
import com.aerospike.examples.arbitraryprecision.model.CubeUpdates;
import com.aerospike.examples.arbitraryprecision.model.DataCube;
import com.aerospike.examples.arbitraryprecision.model.DoubleCellConverter;

public class DataCubeManipulator <T> {
    public static final int TARGET_DATA_SIZE = 10240;
    public static final Value DEFAULT_VALUE = Value.get(0.0);
    private static final MapPolicy MAP_POLICY = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
    
    private final CellConverter<T> cellConverter;
    private final int rows;
    private final int columns;
    private final int rowsPerRecord;
    private final Key key;
    private final IAerospikeClient client;

    public DataCubeManipulator(IAerospikeClient client, Key baseKey, CellConverter<T> cellConverter, int rows, int columns, int rowsPerRecord) {
        this.client = client;
        this.key = baseKey;
        this.rows = rows;
        this.columns = columns;
        this.cellConverter = cellConverter;
        this.rowsPerRecord = rowsPerRecord;
    }

    public DataCubeManipulator(IAerospikeClient client, Key baseKey, CellConverter<T> cellConverter, int rows, int columns) {
        this(client, baseKey, cellConverter, rows, columns, calculateRowsPerRecord(rows, cellConverter));
    }

    /**
     * Define a cube from the data provided 
     * @param cellConverter
     * @param cells
     */
    public DataCubeManipulator(IAerospikeClient client, Key baseKey, CellConverter<T> cellConverter, List<List<T>> cells) {
        this(client, baseKey, cellConverter, cells, 
                calculateRowsPerRecord(cells == null || cells.size() == 0 ? 1 : cells.get(0).size(), cellConverter));
    }
    
    public DataCubeManipulator(IAerospikeClient client, Key baseKey, CellConverter<T> cellConverter, List<List<T>> cells, int rowsPerRecord) {
        this.client = client;
        this.key = baseKey;
        if (cells == null) {
            throw new NullPointerException("Cells cannot be null");
        }
        if (cellConverter == null) {
            throw new NullPointerException("cellConverter cannot be null");
        }
        if (cells.size() == 0) {
            throw new IllegalArgumentException("Cannot define a cube with no rows");
        }
        if (cells.get(0).size() == 0) {
            throw new IllegalArgumentException("Cannot define a cube with no columns");
        }
        this.cellConverter = cellConverter;
        this.rows = cells.size();
        this.columns = cells.get(0).size();
        this.rowsPerRecord = rowsPerRecord;
        
        this.populate(cells);
    }
    
    private static int calculateRowsPerRecord(int rows, CellConverter<?> cellConverter) {
        if (rows <= 0) {
            throw new IllegalArgumentException("rows cannot be " + rows);
        }
        int eachRowSize = rows * cellConverter.estimateSizeInBytes();
        int numRowsPerRecord = Math.max(1, TARGET_DATA_SIZE / eachRowSize);
        return numRowsPerRecord;
    }
    
    /**
     * Get the Key for a particular row of the cube
     * @param rowId
     * @return
     */
    private Key getRowKey(int rowId) {
        if (rowId < 0 || rowId >= rows) {
            throw new IllegalArgumentException(String.format("row must be in the range 0..%d, not %d", rows, rowId));
        }
        int rowValue = (rowId / rowsPerRecord) * rowsPerRecord;
        return new Key(this.key.namespace, this.key.setName, this.key.userKey.toString() + "-row" + rowValue);
    }
    
    /**
     * Get the versioned key for a row record
     * @param rowId - the row ID
     * @param version - the version number
     * @return versioned key in format "userKey:row:version"
     */
    private Key getVersionedRowKey(int rowId, int version) {
        int rowValue = (rowId / rowsPerRecord) * rowsPerRecord;
        return new Key(this.key.namespace, this.key.setName, this.key.userKey.toString() + "-row" + rowValue + ":" + version);
    }
    
    /**
     * Get the versioned key for the cube record
     * @param version - the version number
     * @return versioned key in format "userKey:version"
     */
    private Key getVersionedCubeKey(int version) {
        return new Key(this.key.namespace, this.key.setName, this.key.userKey.toString() + ":" + version);
    }
    
    private String getBinName(int rowId) {
        // return "row-" + rowId;
        // Instead of using the real row id, we use 0, 1, 2,.. rowsPerRecord-1. This is less
        // readable, but allows us to know the bins we need to retrieve at runtime without
        // either an extra trip to the server, or returning the whole latest data when historical
        // is needed
        return "row-" + (rowId - (rowId / rowsPerRecord * rowsPerRecord));
    }
    
    private void populate(List<List<T>> data) {
        long now = new Date().getTime();
        Utils.doInTransaction(client, txn -> {
            BatchPolicy batchPolicy = client.getBatchPolicyDefault();
            batchPolicy.txn = txn;
            batchPolicy.maxConcurrentThreads = 0;
            
            List<BatchRecord> batchItems = new ArrayList<>();
            
            // Base cube record insert
            BatchWritePolicy bwp = client.copyBatchWritePolicyDefault();
            bwp.recordExistsAction = RecordExistsAction.CREATE_ONLY;
            bwp.sendKey = true;
            batchItems.add(new BatchWrite(bwp, key, Operation.array(
                    Operation.put(new Bin("version", 0)),
                    Operation.put(new Bin("versions", Map.of(now, -1))),
                    Operation.put(new Bin("columns", this.columns)),
                    Operation.put(new Bin("rows", this.rows)),
                    Operation.put(new Bin("rowsPerRecord", this.rowsPerRecord))
            )));
            
            // Row-level data. Each record can contain multiple rows, one per bin, so form
            // them into one operation per row
            List<Value> values = new ArrayList<>();
            int numRowsInData = data.size();
            for (int row = 0; row < rows; row += rowsPerRecord) {
                
                // Leave room to store the current version(0) and the version map
                final int extraFields = 2;
                Operation[] ops = new Operation[Math.min(rowsPerRecord, rows - row) + 2];
                ops[0] = Operation.put(new Bin("version", 0));
                // Store the cube version against the row, not the timestamp
                ops[1] = Operation.put(new Bin("versions", Map.of(0, -1)));
                for (int i = 0; i < rowsPerRecord && (row + i < rows); i++) {
                    values.clear();
                    List<T> rowData = (row + i) < numRowsInData ? data.get(row + i) : List.of();
                    for (int col = 0; col < this.columns; col++) {
                        Value val;
                        if (col < rowData.size()) {
                            val = cellConverter.toAerospike(rowData.get(col));
                        }
                        else {
                            val = DEFAULT_VALUE;
                        }
                        values.add(val);
                    }
                    ops[i+extraFields] = Operation.put(new Bin(getBinName(row + i), Value.get(rowData)));
                }
                //
                batchItems.add(new BatchWrite(bwp, getRowKey(row), ops));
            }
            
            client.operate(batchPolicy, batchItems);
            // TODO: Process results
        });
    }
    
    /**
     * Apply versioned changes to the cube. This method:
     * 1. Creates a historical copy of the cube record with incremented version
     * 2. Updates the cube's version and versions map
     * 3. For each affected row record:
     *    - Creates a historical copy with the current row version
     *    - Updates the row's version and versions map (keyed by cube version, not timestamp)
     *    - Applies the actual data changes to the row
     * 
     * All operations are performed in a single transaction with batch operations for efficiency.
     * 
     * @param updates - The changes to apply to the cube
     */
    public void applyChanges(CubeUpdates<T> updates) {
        if (!updates.hasChanges()) {
            return; // Nothing to do
        }
        
        long now = new Date().getTime();
        
        Utils.doInTransaction(client, txn -> {
            // Step 1: Group affected rows by their Aerospike record keys
            long baseTime = System.nanoTime();
            Map<Key, Set<Integer>> rowRecordsToRows = new HashMap<>();
            for (int rowId : updates.getAffectedRows()) {
                Key rowKey = getRowKey(rowId);
                rowRecordsToRows.computeIfAbsent(rowKey, k -> new HashSet<>()).add(rowId);
            }
//System.out.printf(" 1: %,dus", (System.nanoTime() - baseTime)/1000);
            
            // Step 2: Read current versions for cube and all affected row records
            BatchPolicy batchReadPolicy = client.getBatchPolicyDefault();
            batchReadPolicy.txn = txn;
            
            List<BatchRecord> readBatch = new ArrayList<>();
            
            // Read cube fields and latest version
            Operation[] cubeReadOps = new Operation[DataCube.NON_VERSIONS_BINS.length+1];
            cubeReadOps[0] = MapOperation.getByIndex("versions", -1, MapReturnType.KEY);
            for (int i = 0; i < DataCube.NON_VERSIONS_BINS.length; i++) {
                cubeReadOps[1+i] = Operation.get(DataCube.NON_VERSIONS_BINS[i]);
            }
            readBatch.add(new BatchRead(key, cubeReadOps));
//System.out.printf(" 2: %,dus", (System.nanoTime() - baseTime)/1000);
            
            // Read version and versions map for each affected row record
            for (Key rowKey : rowRecordsToRows.keySet()) {
                Set<Integer> rowsToReadInRowKey = rowRecordsToRows.get(rowKey);
                Operation[] rowOperations = new Operation[1+rowsToReadInRowKey.size()];
                rowOperations[0] = MapOperation.getByIndex("versions", -1, MapReturnType.KEY);
                int count = 1;
                for (int actualRow : rowsToReadInRowKey) {
                    rowOperations[count++] = Operation.get(getBinName(actualRow));
                }
                readBatch.add(new BatchRead(rowKey, rowOperations));
            }
//System.out.printf(" 3: %,dus", (System.nanoTime() - baseTime)/1000);

            // Execute batch read
            client.operate(batchReadPolicy, readBatch);
//System.out.printf(" 4: %,dus", (System.nanoTime() - baseTime)/1000);

            // Step 3: Extract current versions and prepare batch writes
            Record cubeRecord = readBatch.get(0).record;
            if (cubeRecord == null) {
                throw new IllegalStateException("Cube record not found");
            }
            
            int currentCubeVersion = cubeRecord.getInt("version");
            int newCubeVersion = currentCubeVersion + 1;
            long cubeVersionMapKeyOfCurrent = cubeRecord.getLong("versions");
            
            // Step 4: Build batch write operations
            List<BatchRecord> writeBatch = new ArrayList<>();
            BatchWritePolicy bwp = client.copyBatchWritePolicyDefault();
            bwp.sendKey = true;
//System.out.printf(" 5: %,dus", (System.nanoTime() - baseTime)/1000);

            // 4a: Copy current cube record to versioned key (excluding versions map)
            Operation[] cubeCopyOps = cubeRecord.bins.entrySet().stream()
                .filter(e -> !"versions".equals(e.getKey()))
                .map(e -> Operation.put(new Bin(e.getKey(), Value.get(e.getValue()))))
                .toArray(Operation[]::new);
//System.out.printf(" 6: %,dus", (System.nanoTime() - baseTime)/1000);

            BatchWritePolicy rowCopyPolicy = client.copyBatchWritePolicyDefault();
            rowCopyPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
            rowCopyPolicy.sendKey = true;
            writeBatch.add(new BatchWrite(rowCopyPolicy, getVersionedCubeKey(currentCubeVersion), cubeCopyOps));
            
            // 4b: Update current cube record's version and versions map
            writeBatch.add(new BatchWrite(bwp, key, Operation.array(
                Operation.put(new Bin("version", newCubeVersion)),
                MapOperation.put(MAP_POLICY, "versions", Value.get(cubeVersionMapKeyOfCurrent), Value.get(currentCubeVersion)),
                MapOperation.put(MAP_POLICY, "versions", Value.get(now), Value.get(-1))
            )));
//System.out.printf(" 7: %,dus", (System.nanoTime() - baseTime)/1000);

            // Step 5: Process each affected row record
            int readIndex = 1; // Start after cube record
            for (Map.Entry<Key, Set<Integer>> entry : rowRecordsToRows.entrySet()) {
                Key rowKey = entry.getKey();
                Set<Integer> affectedRows = entry.getValue();
                
                Record rowRecord = readBatch.get(readIndex++).record;
                if (rowRecord == null) {
                    throw new IllegalStateException("Row record not found: " + rowKey);
                }
                
                int currentRowVersion = rowRecord.getInt("version");
                int newRowVersion = currentRowVersion + 1;
                long rowVersionMapKeyOfCurrent = rowRecord.getLong("versions");
                
                // 5a: Copy current row record to versioned key (excluding versions map)
                Operation[] rowCopyOps = rowRecord.bins.entrySet().stream()
                    .filter(e -> !"versions".equals(e.getKey()))
                    .map(e -> Operation.put(new Bin(e.getKey(), Value.get(e.getValue()))))
                    .toArray(Operation[]::new);
                
                // Use one of the affected row IDs to get the versioned key (They will all map to the same key)
                int representativeRowId = affectedRows.iterator().next();
                writeBatch.add(new BatchWrite(rowCopyPolicy, getVersionedRowKey(representativeRowId, currentRowVersion), rowCopyOps));
                
                // 5b: Build operations to update row record: version, versions map, and data changes
                List<Operation> rowUpdateOps = new ArrayList<>();
                
                // Update version
                rowUpdateOps.add(Operation.put(new Bin("version", newRowVersion)));
                
                // Update versions map - NOTE: Row versions map uses cube version, not timestamp
                rowUpdateOps.add(MapOperation.put(MAP_POLICY, "versions", 
                    Value.get(rowVersionMapKeyOfCurrent), Value.get(currentCubeVersion)));
                rowUpdateOps.add(MapOperation.put(MAP_POLICY, "versions", 
                    Value.get((long)newCubeVersion), Value.get(-1)));
                
                // Apply data changes for each affected row in this record
                for (int rowId : affectedRows) {
                    Map<Integer, T> rowChanges = updates.getRowChanges(rowId);
                    if (rowChanges != null && !rowChanges.isEmpty()) {
                        String binName = getBinName(rowId);
                        for (Map.Entry<Integer, T> change : rowChanges.entrySet()) {
                            int colIndex = change.getKey();
                            if (colIndex >= 0 && colIndex < columns) {
                                // Update this cell's data
                                rowUpdateOps.add(ListOperation.set(binName, colIndex, Value.get(cellConverter.toAerospike(change.getValue()))));
                            }
                        }
                    }
                }
                
                // Add batch write for this row record's updates
                writeBatch.add(new BatchWrite(bwp, rowKey, rowUpdateOps.toArray(new Operation[0])));
            }
//System.out.printf(" 8: %,dus", (System.nanoTime() - baseTime)/1000);

            // Step 6: Execute all batch writes
            BatchPolicy batchWritePolicy = client.getBatchPolicyDefault();
            batchWritePolicy.txn = txn;
            batchWritePolicy.maxConcurrentThreads = 0; // Parallel execution
            
            client.operate(batchWritePolicy, writeBatch);
//System.out.printf(" 9: %,dus (%,d entries)\n", (System.nanoTime() - baseTime)/1000, writeBatch.size());

            // TODO: Process results and handle errors if needed
        });
    }
    
    public DataCube<T> getCube(long time) {
        long timeToUse;
        if (time <= 0) {
            timeToUse = new Date().getTime();
        }
        else {
            timeToUse = time;
        }

        return Utils.doInTransaction(client, txn -> {
            
            // Get the cube. Give the cube is quite small, we will return all the data plus the version before
            // the desired time.
            Operation[] cubeReadOps = new Operation[DataCube.NON_VERSIONS_BINS.length+1];
            cubeReadOps[0] = MapOperation.getByKeyRelativeIndexRange("versions", Value.get(timeToUse), -1, 1, MapReturnType.VALUE);
            for (int i = 0; i < DataCube.NON_VERSIONS_BINS.length; i++) {
                cubeReadOps[1+i] = Operation.get(DataCube.NON_VERSIONS_BINS[i]);
            }
            
            WritePolicy writePolicy = client.getWritePolicyDefault();
            writePolicy.txn = txn;
            Record cubeRecord = client.operate(writePolicy, key, cubeReadOps);
            
            DataCube<T> dataCube = new DataCube<>();
            
            List<Long> versionAtDesiredTime = (List<Long>) cubeRecord.getList("versions");
            if (versionAtDesiredTime.size() == 0) {
                // This is before the cube existed
                return null;
            }
            long versionNeeded = versionAtDesiredTime.get(0);
            long versionToSearch = versionNeeded == -1 ? Integer.MAX_VALUE : versionNeeded + 1;
            
            // Now we know the cube version, loop through each cube row, getting either the data (if this is
            // the current version) or the version to use
            List<BatchRecord> batchReads = new ArrayList<>();
            if (versionNeeded != -1) {
                // Need to load the cube at the right time too, in case there are cube level properties whose version is important
                batchReads.add(new BatchRead(getVersionedCubeKey((int)versionNeeded), true));
            }

            for (int thisRow = 0; thisRow < rows; thisRow+= rowsPerRecord) {
                int operationsThisRow = Math.min(rowsPerRecord, rows - thisRow);
                Operation[] opsForCurrentRow = new Operation[operationsThisRow];
                for (int subRow = 0; subRow < operationsThisRow; subRow++) {
                    opsForCurrentRow[subRow] = ExpOperation.read("row-" + subRow, 
                        Exp.build(
                            Exp.let(
                                Exp.def("desiredVersionList", MapExp.getByKeyRelativeIndexRange(MapReturnType.VALUE,  Exp.val(versionToSearch), Exp.val(-1), Exp.val(1), Exp.mapBin("versions"))),
                                Exp.def("desiredVersion", ListExp.getByIndex(ListReturnType.VALUE, Type.INT, Exp.val(0), Exp.var("desiredVersionList"))),
                                Exp.cond(
                                    Exp.eq(Exp.var("desiredVersion"), Exp.val(-1)),
                                    Exp.listBin(getBinName(thisRow + subRow)),
                                    ListExp.append(ListPolicy.Default, Exp.var("desiredVersion"), Exp.val(List.of("index")))
                                )
//                                Exp.var("desiredVersionList")
                            )
                        )
                        , ExpReadFlags.DEFAULT);
                }
                batchReads.add(new BatchRead(getRowKey(thisRow), opsForCurrentRow));
            }
            BatchPolicy batchPolicy = client.copyBatchPolicyDefault();
            batchPolicy.sendKey = true;
            batchPolicy.txn = txn;
            client.operate(batchPolicy, batchReads);

            if (versionNeeded != -1) {
                cubeRecord = batchReads.get(0).record;
                batchReads.remove(0);
            }
            dataCube.setColumns(cubeRecord.getInt("columns"));
            dataCube.setRows(cubeRecord.getInt("rows"));
            dataCube.setVersion(cubeRecord.getInt("version"));

            dataCube.setCubeData(new ArrayList<List<T>>());
            
            // If the rows returned the actual data, map it. We know if the data came back because
            // the array returned will have 2 elements, the first one of which is a string and the
            // second one is the index we need to get.
            Map<Integer, Long> secondaryReads = null;
            for (int thisRow = 0; thisRow < rows; thisRow+= rowsPerRecord) {
                int operationsThisRow = Math.min(rowsPerRecord, rows - thisRow);
                BatchRecord thisRead = batchReads.get(thisRow/rowsPerRecord);
                if (thisRead.resultCode != ResultCode.OK) {
                    throw new AerospikeException(thisRead.resultCode);
                }
                for (int subRow = 0; subRow < operationsThisRow; subRow++) {
                    List<T> list = (List<T>) thisRead.record.getList(getBinName(thisRow + subRow));
                    if (list.size() == 2 && (list.get(0) instanceof String)) {
                        if (secondaryReads == null) {
                            secondaryReads = new HashMap<>();
                        }
                        // All the bins in this record will return the same thing as they share the same version
                        secondaryReads.put(thisRow, (long)list.get(1));
                        dataCube.getCubeData().add(null);
                    }
                    else {
                        // We have the actual data
                        dataCube.getCubeData().add(list);
                    }
                }
            }
            
            // We only have the current version of the data in the cube now. Get the historical data too
            if (secondaryReads != null) {
                batchReads.clear();
                for (Entry<Integer, Long> row: secondaryReads.entrySet()) {
                    batchReads.add(new BatchRead(getVersionedRowKey(row.getKey(), (int)(long)row.getValue()), true));
                }
                client.operate(batchPolicy, batchReads);
                int count = 0;
                for (Entry<Integer, Long> row: secondaryReads.entrySet()) {
                    int rowOrdinal = row.getKey();
                    BatchRecord thisRead = batchReads.get(count++);
                    Record rec = thisRead.record;
                    for (int i = 0; i < rowsPerRecord; i++) {
                        dataCube.getCubeData().set(rowOrdinal + i, (List<T>)rec.getList(getBinName(rowOrdinal+i)));
                    }
                }
            }
            return dataCube;
        });
    }
    
    public static void main(String[] args) {
        try  (IAerospikeClient baseClient = new AerospikeClient("localhost", 3100)) {
            // Simulate transactions for now.
            IAerospikeClient client = AerospikeClientProxy.wrap(baseClient);
            client.truncate(null, "test", "cube", null);
            
            // Create initial cube
            List<List<Double>> grid = new ArrayList<>();
            for (int row = 0; row < 15; row++) {
                List<Double> rowData = new ArrayList<>();
                grid.add(rowData);
                for (int col = 0; col < 20; col++) {
                    rowData.add((row+1.0) * col);
                }
            }
            Key key = new Key("test", "cube", 1);
            DataCubeManipulator<Double> dcm = new DataCubeManipulator<>(client, key, new DoubleCellConverter(), grid, 1);
            
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            // Apply some changes to test versioning
            CubeUpdates<Double> updates = new CubeUpdates<>();
            updates.updateCell(2, 3, 42.0);  // Change cell at row 2, column 3
            updates.updateCell(2, 5, 99.5);  // Change another cell in same row
            updates.updateCell(8, 10, 123.456);  // Change cell in different row (different record)
            
            System.out.println("Applying versioned changes...");
            dcm.applyChanges(updates);
            System.out.println("Changes applied successfully!");
            
            long queryTime = new Date().getTime() - 500;
            System.out.printf("Cube at time %d is: %s\n", queryTime, dcm.getCube(queryTime));

            queryTime = new Date().getTime();
            System.out.printf("\nCube at time %d is: %s\n", queryTime, dcm.getCube(queryTime));

            // Apply more changes to create version 2
            CubeUpdates<Double> updates2 = new CubeUpdates<>();
            updates2.updateCell(2, 3, 84.0);  // Update same cell again
            updates2.updateCell(0, 0, 1.0);   // Update cell in first row
            List<Double> sequence = IntStream.rangeClosed(1, 20)
                    .mapToDouble(i -> i * 0.1)
                    .boxed() // Box the primitive doubles to Double objects for the List
                    .collect(Collectors.toList());
            updates2.updateRow(8, sequence);
            
            System.out.println("Applying second set of versioned changes...");
            dcm.applyChanges(updates2);
            System.out.println("Second set of changes applied successfully!");
            queryTime = new Date().getTime();
            System.out.printf("\nCube at time %d is: %s\n", queryTime, dcm.getCube(queryTime));
            
            System.out.println("\nVersioning test complete. Check the database with AQL to see:");
            System.out.println("- Cube record with key '1' (current version)");
            System.out.println("- Cube versioned records with keys '1:0', '1:1' (historical versions)");
            System.out.println("- Row records and their versioned copies");
            
            // Test 2 -- create a big cube.
            final int ROWS = 5000;
            final int COLS = 600;
            System.out.println("-------------------------------------------");
            System.out.printf("Testing %,d columns by %,d rows data cube\n", COLS, ROWS);
            System.out.println("-------------------------------------------");
            Random rand = ThreadLocalRandom.current();
            List<List<Double>> cube2 = new ArrayList<>(ROWS);
            for (int row = 0; row < ROWS; row++) {
                List<Double> rowValues = new ArrayList<Double>(COLS);
                cube2.add(rowValues);
                for (int col = 0; col < COLS; col++) {
                    rowValues.add(rand.nextDouble());
                }
            }
            Key key2= new Key("test", "cube", 2);
            long now = System.nanoTime();
            DataCubeManipulator<Double> dcm2 = new DataCubeManipulator<>(client, key2, new DoubleCellConverter(), cube2);
            System.out.printf("Initial insert took %,dus\n", (System.nanoTime() - now) / 1000);
            
            now = System.nanoTime();
            dcm2.getCube(new Date().getTime());
            System.out.printf("Initial read took %,dus\n", (System.nanoTime() - now) / 1000);
            // Do multiple times to eliminate JVM warmup
            for (int i = 0; i < 4; i++) {
                System.out.println("-------------------------------------------");
                System.out.printf("Testing %,d columns by %,d rows data cube\n", COLS, ROWS);
                System.out.println("-------------------------------------------");
                timeApplyChangesAndRead(ROWS, COLS, 1, dcm2);
                timeApplyChangesAndRead(ROWS, COLS, 5, dcm2);
                timeApplyChangesAndRead(ROWS, COLS, 10, dcm2);
                timeApplyChangesAndRead(ROWS, COLS, 50, dcm2);
                timeApplyChangesAndRead(ROWS, COLS, 100, dcm2);
                timeApplyChangesAndRead(ROWS, COLS, 500, dcm2);
                timeApplyChangesAndRead(ROWS, COLS, 1000, dcm2);
                timeApplyChangesAndRead(ROWS, COLS, 5000, dcm2);
                timeApplyChangesAndRead(ROWS, COLS, 10000, dcm2);
                timeApplyChangesAndRead(ROWS, COLS, 50000, dcm2);
                timeApplyChangesAndRead(ROWS, COLS, 100000, dcm2);
                timeApplyChangesAndRead(ROWS, COLS, 500000, dcm2);
            }
        }
    }
    
    private static void timeApplyChangesAndRead(int ROWS, int COLS, int numChanges, DataCubeManipulator<Double> dcm) {
        Random rand = ThreadLocalRandom.current();
        CubeUpdates<Double> updates = new CubeUpdates<>();
        for (int i = 0; i < numChanges; i++) {
            updates.updateCell(rand.nextInt(ROWS), rand.nextInt(COLS), rand.nextDouble());
        }
        
        long now = System.nanoTime();
        dcm.applyChanges(updates);
        long afterChanges = System.nanoTime();
        dcm.getCube(new Date().getTime());
        long afterRead = System.nanoTime();
        System.out.printf("%,8d changes took %,9dus, reading back took %,dus\n", 
                numChanges,
                (afterChanges - now) / 1000,
                (afterRead - afterChanges) / 1000);

    }

}
