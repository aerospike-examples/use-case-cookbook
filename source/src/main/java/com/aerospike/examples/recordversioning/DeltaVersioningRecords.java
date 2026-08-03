package com.aerospike.examples.recordversioning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Txn;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.examples.Parameter;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.Utils;
import com.aerospike.examples.recordversioning.model.TradeBase;
import com.aerospike.generator.Generator;
import com.aerospike.mapper.tools.AeroMapper;

/**
 * Demonstrates delta-based record versioning for {@link TradeBase}: the live record always holds the
 * current effective state, while an audit trail of <em>what changed</em> is stored separately.
 *
 * <h2>Versioning model</h2>
 * <ul>
 *   <li><b>Live record</b> ({@code id}) — the current {@link TradeBase}; carries a {@code version}
 *       integer and a {@code versions} map for point-in-time lookup.</li>
 *   <li><b>Delta records</b> ({@code id:0}, {@code id:1}, …) — immutable audit entries, one per
 *       version. Each stores metadata ({@code description}, {@code user}, {@code changeTs}) and a
 *       {@code changes} list describing bin-level deltas (not a full copy of the record).</li>
 * </ul>
 *
 * <p>The {@code versions} map maps change timestamps to version numbers. The entry whose value is
 * {@code -1} identifies the timestamp of the <em>current</em> effective version; all other entries
 * map a prior change timestamp to the version number that was effective from that point until the
 * next change. Example after three updates:
 * {@code {t0=0, t1=1, t2=2, t3=-1}}.
 *
 * <h2>Delta versioning</h2>
 * Each call to {@link #updateTradeBaseWithDelta} performs one atomic {@code operate} on the live
 * record, then writes a new delta record at {@code id:newVersion}:
 * <ol>
 *   <li>Snapshot each targeted bin into a temporary bin <em>before</em> caller operations run.</li>
 *   <li>Apply the caller's {@link Operation}s.</li>
 *   <li>Compare before/after server-side and classify each bin as {@code Inserted},
 *       {@code Changed}, {@code TypeChanged}, {@code Removed}, or {@code Same}.</li>
 *   <li>Increment {@code version}, update the {@code versions} map, and set {@code updatedDate}.</li>
 *   <li>Persist only non-{@code Same} entries on the delta record's {@code changes} list.</li>
 * </ol>
 *
 * <p>The initial insert follows the same path: delta {@code id:0} lists every bin as
 * {@code Inserted}. When {@link #RECORD_CONTENTS} is enabled, each delta also stores
 * {@code newValue} for changed bins so {@link #reconstructAtVersion} can replay history.
 *
 * <h2>Temporary bins</h2>
 * Temporary bins are written during the operate chain and cleared before it completes. Bin names
 * must stay within Aerospike's 15-character limit.
 * <ul>
 *   <li>{@code _cv} — holds the {@code version} bin value captured <em>before</em> user operations
 *       ({@code -1} when the record does not yet exist); used to increment version and to close the
 *       prior entry in the {@code versions} map.</li>
 *   <li>{@code _t0}, {@code _t1}, … — snapshots of each targeted bin's value before user
 *       operations ({@link #TEMP_BIN_PREFIX}).</li>
 *   <li>{@code _a0}, {@code _a1}, … — comparison result for each targeted bin: one of
 *       {@code Inserted}, {@code Changed}, {@code TypeChanged}, {@code Removed}, or {@code Same}
 *       ({@link #ACTION_BIN_PREFIX}).</li>
 * </ul>
 *
 * <h2>Record size limit</h2>
 * Snapshot bins duplicate targeted bin values on the live record for the duration of the operate.
 * If permanent bins plus temporary copies exceed the namespace {@code max-record-size} (or 8 MB,
 * whichever is lower), the update fails with {@code RECORD_TOO_BIG}. Use this pattern only when
 * steady-state record size is well under half the maximum — e.g. changing a 5 MB blob will fail
 * because the temporary copy doubles that bin on the record.
 */
public class DeltaVersioningRecords implements UseCase {

    /** Bin name paired with its Aerospike particle type for server-side expression comparisons. */
    public static final class BinAndType {
        private final String name;
        private final int particleType;

        private BinAndType(String name, int particleType) {
            this.name = name;
            this.particleType = particleType;
        }

        public static BinAndType of(String name, int particleType) {
            return new BinAndType(name, particleType);
        }

        public String name() {
            return name;
        }

        public int particleType() {
            return particleType;
        }

        /** Maps the Aerospike particle type to the corresponding {@link Exp.Type} for expressions. */
        public Exp.Type type() {
            switch (particleType) {
                case ParticleType.BOOL:
                    return Exp.Type.BOOL;
                case ParticleType.BLOB:
                    return Exp.Type.BLOB;
                case ParticleType.DOUBLE:
                    return Exp.Type.FLOAT;
                case ParticleType.HLL:
                    return Exp.Type.HLL;
                case ParticleType.GEOJSON:
                    return Exp.Type.GEO;
                case ParticleType.INTEGER:
                    return Exp.Type.INT;
                case ParticleType.LIST:
                    return Exp.Type.LIST;
                case ParticleType.MAP:
                    return Exp.Type.MAP;
                case ParticleType.STRING:
                    return Exp.Type.STRING;
                default:
                    return null;
            }
        }
    }

    private static final MapPolicy MAP_POLICY = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
    private static final String TEMP_BIN_PREFIX = "_t";
    private static final String ACTION_BIN_PREFIX = "_a";
    private static final int VERSION_BEFORE_INSERT = -1;

    /** Hard-coded mapping of {@link TradeBase} bin names to Aerospike particle types (dates as INTEGER). */
    private static final Map<String, Integer> TRADE_BASE_BIN_TYPES;
    /** Bins managed internally; caller operations on these are filtered out. */
    private static final Set<String> PROTECTED_BINS;
    static {
        Map<String, Integer> types = new HashMap<>();
        types.put("sourceSystemId", ParticleType.STRING);
        types.put("parentTradeId", ParticleType.INTEGER);
        types.put("extTradeId", ParticleType.STRING);
        types.put("contentId", ParticleType.INTEGER);
        types.put("book", ParticleType.STRING);
        types.put("counterparty", ParticleType.STRING);
        types.put("tradeDate", ParticleType.INTEGER);
        types.put("enteredDate", ParticleType.INTEGER);
        types.put("updatedDate", ParticleType.INTEGER);
        types.put("tradeVersion", ParticleType.INTEGER);
        types.put("recordComplete", ParticleType.BOOL);
        types.put("dataVersion", ParticleType.INTEGER);
        types.put("versions", ParticleType.MAP);
        TRADE_BASE_BIN_TYPES = Collections.unmodifiableMap(types);

        PROTECTED_BINS = new HashSet<>();
        PROTECTED_BINS.add("version");
        PROTECTED_BINS.add("versions");
    }

    private final Parameter<Boolean> RECORD_CONTENTS = new Parameter<>(
            "recordContents",
            false,
            "Store new bin values in delta records to allow record reconstruction");

    private final Parameter<Integer> NUM_RECORDS = new Parameter<>(
            "numRecords",
            100,
            "Number of TradeBase records to create during setup");

    private IAerospikeClient client;
    private AeroMapper mapper;
    private String namespace;
    private String setName;
    private boolean recordContentsOverride;

    public DeltaVersioningRecords() {
    }

    public DeltaVersioningRecords(boolean recordContents) {
        this.recordContentsOverride = recordContents;
    }

    private boolean recordContents() {
        return RECORD_CONTENTS.get() || recordContentsOverride;
    }

    private void initializeNamespacesAndSets(AeroMapper mapper) {
        this.namespace = mapper.getNamespace(TradeBase.class);
        this.setName = mapper.getSet(TradeBase.class);
    }

    @Override
    public String getName() {
        return "Delta Versioning Records";
    }

    @Override
    public String getDescription() {
        return "Maintain an audit trail of TradeBase record changes using delta records instead of full copies. "
                + "The initial insert is recorded as delta version 0 with all bins marked Inserted. Subsequent "
                + "changes store bin-level deltas (Inserted, Changed, TypeChanged, Removed). Optionally stores "
                + "new bin values to support record reconstruction.";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/versioning-records-delta.md";
    }

    @Override
    public String[] getTags() {
        return new String[] {
                "Versioning",
                "Audit Trail",
                "Transactions",
                "Expressions",
                "Map Operations"
        };
    }

    @Override
    public Parameter<?>[] getParams() {
        return new Parameter<?>[] { RECORD_CONTENTS, NUM_RECORDS };
    }

    /**
     * Truncates the set and loads {@link #NUM_RECORDS} {@link TradeBase} records. Each insert is performed via
     * {@link #updateTradeBaseWithDelta} so the initial state is recorded as delta version 0 with
     * all bins marked {@code Inserted}, matching the path used for subsequent updates.
     */
    @Override
    public void setup(IAerospikeClient client, AeroMapper mapper) throws Exception {
        this.client = client;
        this.mapper = mapper;
        initializeNamespacesAndSets(mapper);
        client.truncate(null, namespace, setName, null);

        new Generator().generate(0, NUM_RECORDS.get(), TradeBase.class, trade -> {
            Date now = new Date();
            trade.setDataVersion(0);
            trade.setUpdatedDate(now);

            Map<String, Object> valueMap = mapper.getMappingConverter().convertToMap(trade);
            List<Operation> ops = operationsFromMap(valueMap);

            updateTradeBaseWithDelta(trade.getId(), now.getTime(), "Initial insert", "setup", null, ops);
        }).monitor();
    }

    /**
     * Converts a mapped {@link TradeBase} field map into {@link Operation#put} operations for
     * every non-null, non-protected bin. Used during setup to route initial inserts through the
     * delta path rather than {@code mapper.save}.
     */
    private List<Operation> operationsFromMap(Map<String, Object> valueMap) {
        List<Operation> ops = new ArrayList<>();
        for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
            if (PROTECTED_BINS.contains(entry.getKey())) {
                continue;
            }
            Object value = entry.getValue();
            if (value != null) {
                ops.add(Operation.put(new Bin(entry.getKey(), Value.get(value))));
            }
        }
        return ops;
    }

    private Key formKey(long id) {
        return new Key(namespace, setName, id);
    }

    private Key formKey(long id, int version) {
        return new Key(namespace, setName, id + ":" + version);
    }

    private WritePolicy createWritePolicy(Txn txn) {
        WritePolicy wp = client.copyWritePolicyDefault();
        wp.txn = txn;
        wp.sendKey = true;
        return wp;
    }

    private String tempBinName(int index) {
        return TEMP_BIN_PREFIX + index;
    }

    private String actionBinName(int index) {
        return ACTION_BIN_PREFIX + index;
    }

    /**
     * Copies the current value of a bin into a temporary bin ({@code _tN}) before user operations
     * are applied. If the bin is absent or its particle type does not match the expected type,
     * writes {@code unknown} so the subsequent comparison can classify the change correctly.
     */
    private Operation makeTemporaryCopy(BinAndType binAndType, int index) {
        String tempBin = tempBinName(index);
        return ExpOperation.write(tempBin,
                Exp.build(Exp.cond(
                        Exp.eq(Exp.binType(binAndType.name()), Exp.val(binAndType.particleType())),
                        Exp.bin(binAndType.name(), binAndType.type()),
                        Exp.unknown())),
                ExpWriteFlags.EVAL_NO_FAIL | ExpWriteFlags.POLICY_NO_FAIL);
    }

    /**
     * Compares a bin's value after user operations with the temporary snapshot taken beforehand
     * and writes a status string to {@code _aN}: {@code Inserted}, {@code Changed},
     * {@code TypeChanged}, {@code Removed}, or {@code Same}.
     */
    private Operation compareCopiesOnBin(BinAndType binAndType, int index) {
        String tempBin = tempBinName(index);
        return ExpOperation.read(actionBinName(index),
                Exp.build(Exp.cond(
                        Exp.not(Exp.binExists(binAndType.name())), Exp.val("Removed"),
                        Exp.not(Exp.binExists(tempBin)), Exp.val("Inserted"),
                        Exp.not(Exp.eq(Exp.binType(binAndType.name()), Exp.binType(tempBin))),
                            Exp.val("TypeChanged"),
                        Exp.eq(Exp.bin(binAndType.name(), binAndType.type()),
                                Exp.bin(tempBin, binAndType.type())),
                            Exp.val("Same"), Exp.val("Changed"))),
                ExpReadFlags.EVAL_NO_FAIL);
    }

    /**
     * Removes temporary comparison bins ({@code _t*} and {@code _a*}) from the live record after
     * delta detection completes.
     */
    private List<Operation> removeComparisonTempBins(int binCount) {
        List<Operation> ops = new ArrayList<>();
        for (int i = 0; i < binCount; i++) {
            ops.add(Operation.put(Bin.asNull(tempBinName(i))));
            ops.add(Operation.put(Bin.asNull(actionBinName(i))));
        }
        return ops;
    }

    /**
     * Removes operations targeting {@code version} or {@code versions}, which are managed by this
     * class and must not be modified directly by callers.
     */
    private List<Operation> filterUserOps(List<Operation> userOps) {
        return userOps.stream()
                .filter(op -> op.binName == null || !PROTECTED_BINS.contains(op.binName))
                .collect(Collectors.toList());
    }

    /**
     * Derives the set of bins touched by write operations and resolves each to a
     * {@link BinAndType} using {@link #TRADE_BASE_BIN_TYPES}, falling back to the operation value
     * type when the bin is not in the hard-coded map.
     */
    private List<BinAndType> resolveChangedBins(List<Operation> userOps) {
        Set<String> binNames = new HashSet<>();
        for (Operation op : userOps) {
            if (op.type.isWrite && op.binName != null) {
                binNames.add(op.binName);
            }
        }
        List<BinAndType> changedBins = new ArrayList<>();
        for (String binName : binNames) {
            Integer type = TRADE_BASE_BIN_TYPES.get(binName);
            if (type == null && userOps.stream().anyMatch(op -> binName.equals(op.binName) && op.value != null)) {
                type = userOps.stream()
                        .filter(op -> binName.equals(op.binName) && op.value != null)
                        .findFirst()
                        .map(op -> op.value.getType())
                        .orElse(null);
            }
            if (type == null) {
                throw new IllegalArgumentException("Missing particle type for bin: " + binName);
            }
            changedBins.add(BinAndType.of(binName, type));
        }
        return changedBins;
    }

    /**
     * Returns the final value for a bin from an operate result. When multiple operations touch the
     * same bin, Aerospike returns a list; this method returns the last entry.
     */
    private Object binValueFromOperateResult(Record result, String binName) {
        Object value = result.getValue(binName);
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            return list.isEmpty() ? null : list.get(list.size() - 1);
        }
        return value;
    }

    private int intFromOperateResult(Record result, String binName, int defaultValue) {
        Object value = binValueFromOperateResult(result, binName);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        throw new IllegalStateException("Expected numeric bin " + binName + " but got " + value.getClass());
    }

    /**
     * Reads the comparison status from {@code _aN}. Expression read results may include trailing
     * null entries; this returns the last non-null status string.
     */
    private String readActionStatus(Record result, int index) {
        Object value = result.getValue(actionBinName(index));
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            for (int i = list.size() - 1; i >= 0; i--) {
                Object item = list.get(i);
                if (item != null) {
                    return item.toString();
                }
            }
            return null;
        }
        return value == null ? null : value.toString();
    }

    /**
     * Builds the {@code changes} list persisted on the delta record from comparison results.
     * Entries classified as {@code Same} are omitted. When {@link #recordContents()} is enabled,
     * includes {@code newValue} for all statuses except {@code Removed}.
     *
     * <p>Example: a single update that puts {@code counterparty="CP-1001"} and {@code book="XY"}
     * when both bins previously held different values would produce:
     * <pre>{@code
     * [
     *   {binName=counterparty, status=Changed, newValue=CP-1001},
     *   {binName=book,         status=Changed, newValue=XY}
     * ]
     * }</pre>
     * ({@code newValue} entries appear only when {@link #recordContents()} is enabled.)
     *
     * <p>If {@code book} had already been {@code "XY"}, its comparison status would be
     * {@code Same} and it would be omitted, leaving a single-entry list for {@code counterparty}
     * only. An initial insert marks every written bin as {@code Inserted}; a bin delete would
     * appear as {@code {binName=..., status=Removed}} with no {@code newValue}.
     *
     * @param result      operate result containing {@code _aN} status bins and final bin values
     * @param changedBins bins targeted by this update, in comparison order
     */
    private List<Map<String, Object>> parseChanges(Record result, List<BinAndType> changedBins) {
        List<Map<String, Object>> changes = new ArrayList<>();
        for (int i = 0; i < changedBins.size(); i++) {
            BinAndType binAndType = changedBins.get(i);
            String status = readActionStatus(result, i);
            if (status == null || "Same".equals(status)) {
                continue;
            }
            Map<String, Object> change = new LinkedHashMap<>();
            change.put("binName", binAndType.name());
            change.put("status", status);
            if (recordContents() && !"Removed".equals(status)) {
                change.put("newValue", binValueFromOperateResult(result, binAndType.name()));
            }
            changes.add(change);
        }
        return changes;
    }

    /**
     * Captures the current {@code version} bin into temporary bin {@code _cv} before user
     * operations run. Uses {@link #VERSION_BEFORE_INSERT} when the record does not yet exist.
     */
    private Operation captureCurrentVersion() {
        return ExpOperation.write("_cv",
                Exp.build(Exp.cond(
                        Exp.binExists("version"),
                        Exp.bin("version", Exp.Type.INT),
                        Exp.val(VERSION_BEFORE_INSERT))),
                ExpWriteFlags.EVAL_NO_FAIL | ExpWriteFlags.POLICY_NO_FAIL);
    }

    /**
     * Atomically maintains the point-in-time versions map in the same operate as the data update.
     * On initial insert ({@code _cv == -1}), seeds {@code {changeTs: -1}}. On subsequent updates,
     * finds the key whose value is {@code -1}, closes it to the prior version ({@code _cv}), and
     * appends {@code {changeTs: -1}} for the new effective version.
     */
    private Operation updateVersionsMap(long changeTs) {
        Exp emptyOrExistingMap = Exp.cond(
                Exp.binExists("versions"),
                Exp.mapBin("versions"),
                Exp.val(Collections.emptyMap()));

        Exp initialSeed = MapExp.put(MAP_POLICY, Exp.val(changeTs), Exp.val(-1), emptyOrExistingMap);

        Exp updateExisting = Exp.let(
                Exp.def("map", Exp.mapBin("versions")),
                Exp.def("currentKey", ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(0),
                        MapExp.getByValue(MapReturnType.KEY, Exp.val(-1), Exp.var("map")))),
                Exp.def("closed", MapExp.put(
                        MAP_POLICY,
                        Exp.var("currentKey"),
                        Exp.bin("_cv", Exp.Type.INT),
                        Exp.var("map"))),
                MapExp.put(MAP_POLICY, Exp.val(changeTs), Exp.val(-1), Exp.var("closed")));

        return ExpOperation.write("versions",
                Exp.build(Exp.cond(
                        Exp.eq(Exp.bin("_cv", Exp.Type.INT), Exp.val(VERSION_BEFORE_INSERT)),
                        initialSeed,
                        updateExisting)),
                ExpWriteFlags.EVAL_NO_FAIL | ExpWriteFlags.POLICY_NO_FAIL);
    }

    /**
     * Sets {@code version} to {@code _cv + 1} using a server-side expression, where {@code _cv}
     * holds the version captured before user operations were applied.
     */
    private Operation incrementVersion() {
        return ExpOperation.write("version",
                Exp.build(Exp.add(Exp.bin("_cv", Exp.Type.INT), Exp.val(1))),
                ExpWriteFlags.EVAL_NO_FAIL | ExpWriteFlags.POLICY_NO_FAIL);
    }

    /**
     * Creates an immutable delta audit record at key {@code id:version} containing metadata and
     * the bin-level change list. Uses {@link RecordExistsAction#CREATE_ONLY} so duplicate version
     * keys fail rather than overwrite.
     */
    private void saveDeltaRecord(long id, int version, String description, String user, long changeTs,
            List<Map<String, Object>> changes, WritePolicy writePolicy) {
        writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        client.put(writePolicy, formKey(id, version),
                new Bin("description", description),
                new Bin("user", user),
                new Bin("changeTs", changeTs),
                new Bin("deltaVer", version),
                new Bin("changes", changes));
    }

    /**
     * Applies changes to the live {@link TradeBase} record at key {@code id}, detects bin-level
     * deltas using server-side expressions, and stores the result as a separate delta record at
     * {@code id:version}.
     *
     * <p>The update runs as a single atomic {@code operate} on the live record:
     * <ol>
     *   <li>Capture the current version into {@code _cv}</li>
     *   <li>Snapshot each targeted bin into {@code _tN}, apply caller operations, then compare
     *       before/after into {@code _aN}</li>
     *   <li>Increment {@code version}, update the point-in-time {@code versions} map, and set
     *       {@code updatedDate}</li>
     * </ol>
     * A separate {@code put} then creates the delta record. Operations on {@code version} and
     * {@code versions} in {@code userOps} are ignored.
     *
     * @param id          trade identifier (live record key)
     * @param timestamp   change timestamp; {@code 0} uses the current time
     * @param description human-readable description stored on the delta record
     * @param user        user identifier stored on the delta record
     * @param existingTxn optional transaction; one is created if {@code null}
     * @param userOps     bin operations to apply (puts, adds, etc.)
     * @return the new version number after the update
     */
    public int updateTradeBaseWithDelta(long id, long timestamp, String description, String user, Txn existingTxn,
            List<Operation> userOps) {
        return Utils.doInTransaction(client, existingTxn, txn -> {
            WritePolicy writePolicy = createWritePolicy(txn);
            Key unversionedKey = formKey(id);
            long changeTs = timestamp == 0 ? new Date().getTime() : timestamp;

            List<Operation> filteredOps = filterUserOps(userOps);
            List<BinAndType> changedBins = resolveChangedBins(filteredOps);

            List<Operation> ops = new ArrayList<>();
            ops.add(Operation.get("version"));
            ops.add(captureCurrentVersion());
            for (BinAndType binAndType : changedBins) {
                if (binAndType.particleType() == ParticleType.MAP) {
                    ops.add(MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED, 0), binAndType.name()));
                }
            }
            for (int i = 0; i < changedBins.size(); i++) {
                ops.add(makeTemporaryCopy(changedBins.get(i), i));
            }
            ops.addAll(filteredOps);
            for (int i = 0; i < changedBins.size(); i++) {
                ops.add(compareCopiesOnBin(changedBins.get(i), i));
            }
            if (recordContents()) {
                for (BinAndType binAndType : changedBins) {
                    ops.add(Operation.get(binAndType.name()));
                }
            }
            ops.addAll(removeComparisonTempBins(changedBins.size()));
            ops.add(incrementVersion());
            ops.add(updateVersionsMap(changeTs));
            ops.add(Operation.put(new Bin("updatedDate", changeTs)));
            ops.add(Operation.put(Bin.asNull("_cv")));
            ops.add(Operation.get("version"));

            writePolicy.recordExistsAction = client.exists(null, unversionedKey)
                    ? RecordExistsAction.UPDATE
                    : RecordExistsAction.CREATE_ONLY;
            Record operateResult = client.operate(writePolicy, unversionedKey, ops.toArray(new Operation[0]));

            List<Map<String, Object>> changes = parseChanges(operateResult, changedBins);
            int newVersion = intFromOperateResult(operateResult, "version", 0);

            writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
            saveDeltaRecord(id, newVersion, description, user, changeTs, changes, writePolicy);

            return newVersion;
        });
    }

    /**
     * Returns all delta records for a {@link TradeBase}, from version {@code 0} through the current
     * live version. Delta records are stored at keys {@code id:0}, {@code id:1}, …; the live record
     * at {@code id} holds the current effective state.
     *
     * @param id trade identifier
     * @throws IllegalArgumentException if no live record exists for {@code id}
     */
    public List<Record> getAuditTrail(long id) {
        Record current = client.get(null, formKey(id));
        if (current == null) {
            throw new IllegalArgumentException("No TradeBase record with id: " + id);
        }
        int currentVersion = current.getInt("version");
        List<Record> trail = new ArrayList<>();
        for (int version = 0; version <= currentVersion; version++) {
            Record delta = client.get(null, formKey(id, version));
            if (delta != null) {
                trail.add(delta);
            }
        }
        return trail;
    }

    /**
     * Reconstructs a {@link TradeBase} as it existed at {@code targetVersion} by applying deltas
     * from version {@code 0} onward. Each delta's {@code changes} list is replayed in order:
     * {@code Inserted}/{@code Changed}/{@code TypeChanged} set bin values, {@code Removed} deletes
     * them.
     *
     * @param id            trade identifier
     * @param targetVersion version to reconstruct (inclusive)
     * @return a {@link Record} containing the reconstructed bins
     * @throws IllegalStateException if {@link #recordContents()} is disabled
     * @throws IllegalArgumentException if a required delta record is missing
     */
    public Record reconstructAtVersion(long id, int targetVersion) {
        if (!recordContents()) {
            throw new IllegalStateException("Record reconstruction requires recordContents to be enabled");
        }
        Map<String, Object> reconstructed = new HashMap<>();

        for (int version = 0; version <= targetVersion; version++) {
            Record delta = client.get(null, formKey(id, version));
            if (delta == null) {
                throw new IllegalArgumentException("Missing delta record for version " + version);
            }
            List<Map<String, Object>> changes = (List<Map<String, Object>>) delta.getList("changes");
            if (changes == null) {
                continue;
            }
            for (Map<String, Object> change : changes) {
                String binName = (String) change.get("binName");
                String status = (String) change.get("status");
                if ("Removed".equals(status)) {
                    reconstructed.remove(binName);
                } else {
                    reconstructed.put(binName, change.get("newValue"));
                }
            }
        }
        reconstructed.put("version", targetVersion);
        Record current = client.get(null, formKey(id));
        int generation = current == null ? 0 : current.generation;
        return new Record(reconstructed, generation, 0);
    }

    /** Prints the audit trail for a trade to stdout (demonstration helper). */
    private void printAuditTrail(long id) {
        System.out.println("Audit trail for TradeBase id " + id + ":");
        for (Record rec : getAuditTrail(id)) {
            System.out.printf("  Delta v%d at %d by %s: %s%n",
                    rec.getInt("deltaVer"),
                    rec.getLong("changeTs"),
                    rec.getString("user"),
                    rec.getString("description"));
            List<Map<String, Object>> changes = (List<Map<String, Object>>) rec.getList("changes");
            if (changes != null) {
                for (Map<String, Object> change : changes) {
                    System.out.printf("    %s: %s%n", change.get("binName"), change.get("status"));
                }
            }
        }
    }

    /**
     * Demonstrates sequential updates (including a multi-bin change), prints the audit trail and
     * versions map, and optionally reconstructs the record when {@code recordContents} is enabled.
     */
    @Override
    public void run(IAerospikeClient client, AeroMapper mapper) throws Exception {
        this.client = client;
        this.mapper = mapper;
        initializeNamespacesAndSets(mapper);

        final long tradeId = 2;

        updateTradeBaseWithDelta(tradeId, 0, "Increment trade version", "batch-user", null,
                List.of(Operation.add(new Bin("tradeVersion", 1))));

        updateTradeBaseWithDelta(tradeId, 0, "Update counterparty and book", "alice", null,
                List.of(
                        Operation.put(new Bin("counterparty", "CP-1001")),
                        Operation.put(new Bin("book", "XY"))));

        updateTradeBaseWithDelta(tradeId, 0, "Mark record complete", "bob", null,
                List.of(Operation.put(new Bin("recordComplete", true))));

        printAuditTrail(tradeId);

        System.out.println("\nVersion map:");
        System.out.println(client.get(null, formKey(tradeId), "versions"));

        if (recordContents()) {
            TradeBase current = mapper.read(TradeBase.class, tradeId);
            System.out.printf("%nReconstructed at version %d:%n", current.getVersion());
            System.out.println(reconstructAtVersion(tradeId, current.getVersion()).bins);
        } else {
            System.out.println("\nRecord reconstruction disabled (set recordContents=true to enable).");
        }
    }
}
