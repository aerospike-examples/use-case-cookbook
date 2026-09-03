package com.aerospike.examples.recordversioning;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import com.aerospike.client.sdk.Bin;
import com.aerospike.client.sdk.ChainableOperationBuilder;
import com.aerospike.client.sdk.Operation;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordMapper;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.TypedDataSet;
import com.aerospike.client.sdk.TypedKey;
import com.aerospike.client.sdk.Value;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.client.sdk.cdt.MapPolicy;
import com.aerospike.client.sdk.cdt.MapReturnType;
import com.aerospike.client.sdk.cdt.MapWriteMode;
import com.aerospike.client.sdk.command.ParticleType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.ExpOperation;
import com.aerospike.client.sdk.exp.ExpReadFlags;
import com.aerospike.client.sdk.exp.ExpWriteFlags;
import com.aerospike.client.sdk.exp.ListExp;
import com.aerospike.client.sdk.exp.MapExp;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.recordversioning.model.TradeBase;
import com.aerospike.mapper.tools.AeroMapper;

/**
 * SDK port of the legacy {@code DeltaVersioningRecords} (see ../../java). Same {@link TradeBase}
 * live-record-plus-{@code versions}-map model as {@link VersioningRecords}, but instead of copying
 * the entire prior record to a historical key on every change, each update writes a small
 * <em>delta</em> audit record ({@code id:version}) describing only which bins changed and how
 * ({@code Inserted}/{@code Changed}/{@code TypeChanged}/{@code Removed}/{@code Same}), plus their
 * new values - enough to reconstruct any past version by replaying deltas from version 0 forward.
 * <p/>
 * {@link #updateTradeBaseWithDelta} takes a caller-supplied {@code List<Operation>}, not a map of
 * literal new values - the same contract the legacy version has. This matters: a caller can express
 * an increment ({@code Operation.add}), a nested list/map write at an arbitrary {@code CTX} depth,
 * or any other CDT operation, without ever knowing what the resulting value will be. Diffing can't
 * compare "new value" against "old value" the way {@link VersioningRecords} does, then - instead it
 * mirrors the legacy version's technique exactly: for each bin the caller's operations touch,
 * snapshot its current value into a temporary bin ({@code _t0}, {@code _t1}, ...) with an
 * {@code Exp} write, apply the caller's own operations unmodified, then compare each temp snapshot
 * against the live bin's new value with an {@code Exp} read, writing the classification to another
 * temporary bin ({@code _a0}, {@code _a1}, ...). Snapshot, apply, compare, and this class's own
 * {@code version}/{@code versions} bookkeeping all run as one {@code operate} call.
 * <p/>
 * Closing the {@code versions} map's entry currently marked -1 and opening a new one needs a key
 * discovered at runtime by value ({@code MapExp.getByValue}), which AEL can't express in one call -
 * selector operands / collection-literal keys must be static literals (AEL_CANONICAL_REFERENCE.md
 * §4.2, §5), so a discovered key can't then address a write. This falls back to the classic
 * {@code Exp}/{@code MapExp}/{@code ExpOperation} builder API, as does the snapshot/compare diffing
 * above it and the {@code version} increment - not by choice but because both hit real limitations
 * in the current SDK build, confirmed by isolated testing against a live cluster rather than assumed
 * from the language reference: a {@code when(...)} branch returning the reserved {@code unknown}
 * literal (needed for "snapshot this bin's value, or {@code unknown} if it's missing/wrong-typed")
 * fails with a parameter error despite AEL_CANONICAL_REFERENCE.md §9.1 documenting it as valid, and
 * comparing two bin paths directly ({@code $.a == $.b}, needed to compare a live bin against its
 * temp-bin snapshot) fails the same way. Both would otherwise have been expressible in AEL.
 * {@link #updateTradeBaseWithDelta} does still use one genuine AEL op alongside all the {@code Exp}
 * and the caller's own opaque operations, in the same {@code operate} call: reading {@code version}'s
 * pre-update value, which needs neither {@code unknown} branches nor bin-to-bin comparison. It's
 * read before (not after) the {@code Exp} op that increments it, since an AEL read of a bin an
 * {@code Exp} op already wrote in the same call fails validation, but the reverse order doesn't.
 * Doing all of this in one {@code operate} call keeps the round-trip count matching the legacy
 * version's (one call for the live record, one for the delta audit record).
 */
public class DeltaVersioningRecords implements UseCase {

    private static final int NUM_RECORDS = 100;
    private static final String TEMP_BIN_PREFIX = "_t";
    private static final String ACTION_BIN_PREFIX = "_a";
    private static final Set<String> PROTECTED_BINS = Set.of("version", "versions");
    private static final MapPolicy VERSIONS_MAP_POLICY = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE);

    /** Hard-coded mapping of {@link TradeBase} bin names to Aerospike particle types (dates as INTEGER). */
    private static final Map<String, Integer> TRADE_BASE_BIN_TYPES = Map.ofEntries(
            Map.entry("sourceSystemId", ParticleType.STRING),
            Map.entry("parentTradeId", ParticleType.INTEGER),
            Map.entry("extTradeId", ParticleType.STRING),
            Map.entry("contentId", ParticleType.INTEGER),
            Map.entry("book", ParticleType.STRING),
            Map.entry("counterparty", ParticleType.STRING),
            Map.entry("tradeDate", ParticleType.INTEGER),
            Map.entry("enteredDate", ParticleType.INTEGER),
            Map.entry("updatedDate", ParticleType.INTEGER),
            Map.entry("tradeVersion", ParticleType.INTEGER),
            Map.entry("recordComplete", ParticleType.BOOL),
            Map.entry("dataVersion", ParticleType.INTEGER));

    private static final String[] SOURCE_SYSTEMS = {"FIX", "MANUAL", "BLOOMBERG", "REUTERS"};
    private static final String[] BOOKS = {"EQ-DESK-1", "FX-DESK-2", "RATES-DESK-3", "CREDIT-DESK-4"};
    private static final String[] COUNTERPARTIES = {"Acme Capital", "Northwind Bank", "Contoso Securities", "Fabrikam Trading"};

    /** Bin name paired with its Aerospike particle type, for server-side snapshot/compare expressions. */
    private static final class BinAndType {
        final String name;
        final int particleType;

        BinAndType(String name, int particleType) {
            this.name = name;
            this.particleType = particleType;
        }

        /** Maps the Aerospike particle type to the corresponding {@link Exp.Type} for expressions. */
        Exp.Type expType() {
            return switch (particleType) {
                case ParticleType.BOOL -> Exp.Type.BOOL;
                case ParticleType.BLOB -> Exp.Type.BLOB;
                case ParticleType.DOUBLE -> Exp.Type.FLOAT;
                case ParticleType.HLL -> Exp.Type.HLL;
                case ParticleType.GEOJSON -> Exp.Type.GEO;
                case ParticleType.INTEGER -> Exp.Type.INT;
                case ParticleType.LIST -> Exp.Type.LIST;
                case ParticleType.MAP -> Exp.Type.MAP;
                case ParticleType.STRING -> Exp.Type.STRING;
                default -> null;
            };
        }
    }

    @Override
    public String getName() {
        return "Delta Versioning Records";
    }

    @Override
    public String getDescription() {
        return "Maintain an audit trail of TradeBase record changes using delta records instead of full copies. "
                + "The initial insert is recorded as delta version 0 with all bins marked Inserted. Subsequent "
                + "changes store bin-level deltas (Inserted, Changed, TypeChanged, Removed), including new values, so any past "
                + "version can be reconstructed by replaying deltas from version 0 forward.";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/versioning-records-delta.md";
    }

    private TypedDataSet<TradeBase> tradeBases;

    private void init(AeroMapper mapper) {
        tradeBases = mapper.getTypedDataSet(TradeBase.class);
    }

    private TypedKey<TradeBase> formKey(long id) {
        return tradeBases.id(id);
    }

    private TypedKey<TradeBase> formKey(long id, int version) {
        return tradeBases.id(id + ":" + version);
    }

    @Override
    public void setup(Session session, AeroMapper mapper) throws Exception {
        init(mapper);
        session.truncate(tradeBases);

        System.out.printf("Generating %,d trades%n", NUM_RECORDS);
        RecordMapper<TradeBase> recordMapper = session.getRecordMappingFactory().getMapper(TradeBase.class);
        for (long id = 0; id < NUM_RECORDS; id++) {
            TradeBase trade = randomTradeBase(id);
            Map<String, Object> initial = recordMapper.toMap(trade);
            initial.remove("version");
            initial.remove("versions");
            // ".type" is mapper-injected type-discriminator metadata, not a real trade field.
            initial.remove(".type");
            updateTradeBaseWithDelta(session, id, System.currentTimeMillis(), "Initial insert", "setup",
                    operationsFromMap(initial));
        }
    }

    /** Converts a mapped field map into {@code Operation.put} operations for every non-null bin. */
    private List<Operation> operationsFromMap(Map<String, Object> valueMap) {
        List<Operation> ops = new ArrayList<>();
        for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
            if (entry.getValue() != null) {
                ops.add(Operation.put(new Bin(entry.getKey(), Value.get(entry.getValue()))));
            }
        }
        return ops;
    }

    private TradeBase randomTradeBase(long id) {
        TradeBase trade = new TradeBase();
        trade.setId(id);
        trade.setSourceSystemId(SOURCE_SYSTEMS[ThreadLocalRandom.current().nextInt(SOURCE_SYSTEMS.length)]);
        trade.setParentTradeId(0);
        trade.setExtTradeId("EXT-" + ThreadLocalRandom.current().nextInt(1_000_000));
        trade.setContentId(id);
        trade.setBook(BOOKS[ThreadLocalRandom.current().nextInt(BOOKS.length)]);
        trade.setCounterparty(COUNTERPARTIES[ThreadLocalRandom.current().nextInt(COUNTERPARTIES.length)]);
        long now = System.currentTimeMillis();
        trade.setTradeDate(new Date(now - ThreadLocalRandom.current().nextLong(0, 30L * 24 * 60 * 60 * 1000)));
        trade.setEnteredDate(trade.getTradeDate());
        trade.setTradeVersion(0);
        trade.setRecordComplete(false);
        trade.setDataVersion(0);
        return trade;
    }

    private static String tempBinName(int index) {
        return TEMP_BIN_PREFIX + index;
    }

    private static String actionBinName(int index) {
        return ACTION_BIN_PREFIX + index;
    }

    /** Removes operations targeting {@code version} or {@code versions}, managed internally by this class. */
    private static List<Operation> filterUserOps(List<Operation> userOps) {
        List<Operation> filtered = new ArrayList<>();
        for (Operation op : userOps) {
            if (op.binName == null || !PROTECTED_BINS.contains(op.binName)) {
                filtered.add(op);
            }
        }
        return filtered;
    }

    /**
     * Derives the set of bins touched by write operations and resolves each to a
     * {@link BinAndType} using {@link #TRADE_BASE_BIN_TYPES}, falling back to the operation's own
     * {@link Value} type when the bin isn't in the hard-coded map.
     */
    private static List<BinAndType> resolveChangedBins(List<Operation> userOps) {
        Set<String> binNames = new HashSet<>();
        for (Operation op : userOps) {
            if (op.type.isWrite && op.binName != null) {
                binNames.add(op.binName);
            }
        }
        List<BinAndType> changedBins = new ArrayList<>();
        for (String binName : binNames) {
            Integer type = TRADE_BASE_BIN_TYPES.get(binName);
            if (type == null) {
                type = userOps.stream()
                        .filter(op -> binName.equals(op.binName) && op.value != null)
                        .findFirst()
                        .map(op -> op.value.getType())
                        .orElseThrow(() -> new IllegalArgumentException("Missing particle type for bin: " + binName));
            }
            changedBins.add(new BinAndType(binName, type));
        }
        return changedBins;
    }

    /**
     * Copies the current value of a bin into a temporary bin ({@code _tN}) before the caller's
     * operations run. If the bin is absent or its particle type doesn't match the expected type,
     * writes {@code unknown} so the subsequent comparison classifies the change correctly.
     * {@code Exp.binType} throws (rather than yielding a comparable sentinel) when the bin doesn't
     * exist, unlike the legacy client - checked with {@code Exp.binExists} first to avoid that.
     */
    private static Operation makeTemporaryCopy(BinAndType binAndType, int index) {
        return ExpOperation.write(tempBinName(index),
                Exp.build(Exp.cond(
                        Exp.not(Exp.binExists(binAndType.name)), Exp.unknown(),
                        Exp.eq(Exp.binType(binAndType.name), Exp.val(binAndType.particleType)),
                            Exp.bin(binAndType.name, binAndType.expType()),
                        Exp.unknown())),
                ExpWriteFlags.EVAL_NO_FAIL | ExpWriteFlags.POLICY_NO_FAIL);
    }

    /**
     * Compares a bin's value after the caller's operations with the temporary snapshot taken
     * beforehand and writes a status string to {@code _aN}: {@code Inserted}, {@code Changed},
     * {@code TypeChanged}, {@code Removed}, or {@code Same}.
     */
    private static Operation compareCopiesOnBin(BinAndType binAndType, int index) {
        String tempBin = tempBinName(index);
        return ExpOperation.read(actionBinName(index),
                Exp.build(Exp.cond(
                        Exp.not(Exp.binExists(binAndType.name)), Exp.val("Removed"),
                        Exp.not(Exp.binExists(tempBin)), Exp.val("Inserted"),
                        Exp.not(Exp.eq(Exp.binType(binAndType.name), Exp.binType(tempBin))), Exp.val("TypeChanged"),
                        Exp.eq(Exp.bin(binAndType.name, binAndType.expType()), Exp.bin(tempBin, binAndType.expType())),
                            Exp.val("Same"), Exp.val("Changed"))),
                ExpReadFlags.EVAL_NO_FAIL);
    }

    /** Removes temporary comparison bins ({@code _t*} and {@code _a*}) after delta detection completes. */
    private static List<Operation> removeComparisonTempBins(int binCount) {
        List<Operation> ops = new ArrayList<>();
        for (int i = 0; i < binCount; i++) {
            ops.add(Operation.put(Bin.asNull(tempBinName(i))));
            ops.add(Operation.put(Bin.asNull(actionBinName(i))));
        }
        return ops;
    }

    /**
     * Returns the final value for a bin from an operate result. When multiple operations touch the
     * same bin (the caller's write plus this method's own read-back), Aerospike returns a list;
     * this returns the last entry.
     */
    private static Object binValueFromOperateResult(Record result, String binName) {
        Object value = result.bins.get(binName);
        if (value instanceof List<?> list) {
            return list.isEmpty() ? null : list.get(list.size() - 1);
        }
        return value;
    }

    /**
     * Reads the comparison status from {@code _aN}. The action bin is nulled out by
     * {@link #removeComparisonTempBins} after the compare step, so - unlike
     * {@link #binValueFromOperateResult}, where the last multi-result entry is the answer - this
     * scans backwards from the end and returns the last non-null entry, skipping that trailing null.
     */
    private static String readActionStatus(Record result, int index) {
        Object value = result.bins.get(actionBinName(index));
        if (value instanceof List<?> list) {
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

    /** The live record's current {@code version}, or -1 if the record (and hence the bin) is new. */
    private static Exp currentVersionExp() {
        return Exp.cond(Exp.not(Exp.binExists("version")), Exp.val(-1L), Exp.intBin("version"));
    }

    /**
     * Computes the {@code versions} map's new value: closes the entry currently marked -1 (setting
     * it to the version being closed out) and opens a new one, marked -1, at {@code changeTs} - or
     * at {@code oldKey + 1} if that would collide with (or not sort after) the entry being closed,
     * which happens when successive updates to the same trade land in the same millisecond;
     * {@code changeTs} is millisecond-resolution wall-clock time, not a strictly increasing counter.
     * The entry to close is discovered by value ({@code MapExp.getByValue}, AEL can't address a
     * write to a runtime-discovered key - see the class javadoc), and the whole thing short-circuits
     * to a fresh one-entry map when {@code versions} doesn't exist yet, since {@code Exp.mapBin} on
     * a missing bin throws rather than yielding an empty map.
     */
    private static Exp buildVersionsExp(long changeTs) {
        Exp closeAndReopen = Exp.let(
                Exp.def("oldKeyList", MapExp.getByValue(MapReturnType.KEY, Exp.val(-1L), Exp.mapBin("versions"))),
                Exp.def("hasOldKey", Exp.gt(ListExp.size(Exp.var("oldKeyList")), Exp.val(0))),
                Exp.def("oldKey", Exp.cond(Exp.var("hasOldKey"),
                        ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.val(0), Exp.var("oldKeyList")),
                        Exp.val(0L))),
                Exp.def("newKey", Exp.cond(Exp.var("hasOldKey"),
                        Exp.max(Exp.val(changeTs), Exp.add(Exp.var("oldKey"), Exp.val(1L))),
                        Exp.val(changeTs))),
                Exp.cond(
                        Exp.var("hasOldKey"),
                        MapExp.put(VERSIONS_MAP_POLICY, Exp.var("newKey"), Exp.val(-1L),
                                MapExp.put(VERSIONS_MAP_POLICY, Exp.var("oldKey"), currentVersionExp(), Exp.mapBin("versions"))),
                        MapExp.put(VERSIONS_MAP_POLICY, Exp.var("newKey"), Exp.val(-1L), Exp.mapBin("versions"))));
        Exp freshMap = MapExp.put(VERSIONS_MAP_POLICY, Exp.val(changeTs), Exp.val(-1L), Exp.val(Map.of()));
        return Exp.cond(Exp.not(Exp.binExists("versions")), freshMap, closeAndReopen);
    }

    /**
     * Applies the caller's {@link Operation}s to the live {@link TradeBase} record at key {@code
     * id}, detects the bin-level delta server-side (see the class javadoc), and stores the result
     * as a new immutable delta audit record at {@code id:newVersion} - all inside one transaction.
     * @param session - The Session used to access the database.
     * @param id - The trade identifier (live record key).
     * @param timestamp - The change timestamp; 0 uses the current time.
     * @param description - Human-readable description stored on the delta record.
     * @param user - User identifier stored on the delta record.
     * @param userOps - The operations to apply. Operations on {@code version}/{@code versions} are
     * ignored - those bins are managed internally.
     * @return the new version number
     */
    public int updateTradeBaseWithDelta(Session session, long id, long timestamp, String description, String user,
            List<Operation> userOps) {
        return session.doInTransactionReturning(tx -> {
            TypedKey<TradeBase> key = formKey(id);
            long changeTs = timestamp == 0 ? System.currentTimeMillis() : timestamp;

            List<Operation> filteredOps = filterUserOps(userOps);
            List<BinAndType> changedBins = resolveChangedBins(filteredOps);

            List<Operation> ops = new ArrayList<>();
            for (int i = 0; i < changedBins.size(); i++) {
                ops.add(makeTemporaryCopy(changedBins.get(i), i));
            }
            ops.addAll(filteredOps);
            for (int i = 0; i < changedBins.size(); i++) {
                ops.add(compareCopiesOnBin(changedBins.get(i), i));
            }
            for (BinAndType binAndType : changedBins) {
                ops.add(Operation.get(binAndType.name));
            }
            ops.addAll(removeComparisonTempBins(changedBins.size()));
            ops.add(ExpOperation.write("versions", Exp.build(buildVersionsExp(changeTs)), ExpWriteFlags.DEFAULT));
            ops.add(ExpOperation.write("version", Exp.build(Exp.add(currentVersionExp(), Exp.val(1L))), ExpWriteFlags.DEFAULT));
            ops.add(Operation.put(new Bin("updatedDate", changeTs)));

            // An AEL read of "version" - the one genuinely AEL-expressible piece of this method, see
            // the class javadoc - placed before the Exp increment write above touches it, since an
            // AEL read of a bin an Exp op already wrote in the same call fails; reading it first (its
            // pre-increment value) and adding 1 client-side avoids needing a second read afterward.
            ChainableOperationBuilder op = tx.upsert(key)
                    .bin("version$old").selectFrom("when($.version.exists() == false => -1, default => $.version)")
                    .appendOperations(ops.toArray(new Operation[0]));
            Record diffResult = op.execute().getFirstRecord();

            List<Map<String, Object>> changes = new ArrayList<>();
            for (int i = 0; i < changedBins.size(); i++) {
                BinAndType binAndType = changedBins.get(i);
                String status = readActionStatus(diffResult, i);
                if (status == null || "Same".equals(status)) {
                    continue;
                }
                Map<String, Object> change = new LinkedHashMap<>();
                change.put("binName", binAndType.name);
                change.put("status", status);
                if (!"Removed".equals(status)) {
                    change.put("newValue", binValueFromOperateResult(diffResult, binAndType.name));
                }
                changes.add(change);
            }
            int newVersion = (int) diffResult.getLong("version$old") + 1;

            tx.insert(formKey(id, newVersion))
                    .bin("description").setTo(description)
                    .bin("user").setTo(user)
                    .bin("changeTs").setTo(changeTs)
                    .bin("deltaVer").setTo((long) newVersion)
                    .bin("changes").setTo(changes)
                    .execute();

            return newVersion;
        });
    }

    /**
     * Returns all delta records for a trade, from version 0 through its current live version.
     * Delta records are stored at keys {@code id:0}, {@code id:1}, ...; the live record at
     * {@code id} holds the current effective state.
     * @param session - The Session used to access the database.
     * @param id - The trade identifier.
     * @return the trade's delta audit records, in version order.
     * @throws NullPointerException if no live record exists for {@code id}.
     */
    public List<Record> getAuditTrail(Session session, long id) {
        Record current = session.query(formKey(id)).execute().getFirstRecord();
        int currentVersion = current.getInt("version");
        List<Record> trail = new ArrayList<>();
        for (int version = 0; version <= currentVersion; version++) {
            Record record = session.query(formKey(id, version)).execute().getFirstRecord();
            if (record != null) {
                trail.add(record);
            }
        }
        return trail;
    }

    /**
     * Reconstructs the bin values of a trade as they existed at {@code targetVersion} by replaying
     * deltas from version 0 onward. Each delta's {@code changes} list is replayed in order:
     * {@code Inserted}/{@code Changed} set bin values, {@code Removed} deletes them.
     * @param session - The Session used to access the database.
     * @param id - The trade identifier.
     * @param targetVersion - The version to reconstruct (inclusive).
     * @return the reconstructed bin values, as they existed at {@code targetVersion}.
     * @throws IllegalArgumentException if a required delta record is missing.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> reconstructAtVersion(Session session, long id, int targetVersion) {
        Map<String, Object> reconstructed = new HashMap<>();
        for (int version = 0; version <= targetVersion; version++) {
            Record record = session.query(formKey(id, version)).execute().getFirstRecord();
            if (record == null) {
                throw new IllegalArgumentException("Missing delta record for version " + version);
            }
            List<?> changes = record.getList("changes");
            if (changes == null) {
                continue;
            }
            for (Object item : changes) {
                Map<String, Object> change = (Map<String, Object>) item;
                String binName = (String) change.get("binName");
                String status = (String) change.get("status");
                if ("Removed".equals(status)) {
                    reconstructed.remove(binName);
                }
                else {
                    reconstructed.put(binName, change.get("newValue"));
                }
            }
        }
        reconstructed.put("version", (long) targetVersion);
        return reconstructed;
    }

    @SuppressWarnings("unchecked")
    private void printAuditTrail(Session session, long id) {
        System.out.println("Audit trail for TradeBase id " + id + ":");
        for (Record rec : getAuditTrail(session, id)) {
            System.out.printf("  Delta v%d at %d by %s: %s%n",
                    rec.getInt("deltaVer"), rec.getLong("changeTs"), rec.getString("user"), rec.getString("description"));
            List<?> changes = rec.getList("changes");
            if (changes != null) {
                for (Object item : changes) {
                    Map<String, Object> change = (Map<String, Object>) item;
                    System.out.printf("    %s: %s%n", change.get("binName"), change.get("status"));
                }
            }
        }
    }

    @Override
    public void run(Session session, AeroMapper mapper) throws Exception {
        init(mapper);
        final long tradeId = 2;

        // An increment - the caller never learns (or needs to supply) the resulting value; the
        // snapshot-before/compare-after diff still correctly classifies it as Changed (or Same, on
        // the rare chance an increment lands back on the same value).
        updateTradeBaseWithDelta(session, tradeId, 0, "Increment trade version", "batch-user",
                List.of(Operation.add(new Bin("tradeVersion", 1L))));

        updateTradeBaseWithDelta(session, tradeId, 0, "Update counterparty and book", "alice",
                List.of(Operation.put(new Bin("counterparty", "CP-1001")), Operation.put(new Bin("book", "XY"))));

        updateTradeBaseWithDelta(session, tradeId, 0, "Mark record complete", "bob",
                List.of(Operation.put(new Bin("recordComplete", true))));

        printAuditTrail(session, tradeId);

        System.out.println("\nVersion map:");
        Record versionsRecord = session.query(formKey(tradeId)).readingOnlyBins("versions").execute().getFirstRecord();
        System.out.println(versionsRecord == null ? null : versionsRecord.getMap("versions"));

        Record current = session.query(formKey(tradeId)).execute().getFirstRecord();
        int version = current.getInt("version");
        System.out.printf("%nReconstructed at version %d:%n", version);
        System.out.println(reconstructAtVersion(session, tradeId, version));
    }
}
