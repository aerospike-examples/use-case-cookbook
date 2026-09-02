package com.aerospike.examples.recordversioning;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ChainableOperationBuilder;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordMapper;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.TypedDataSet;
import com.aerospike.client.sdk.TypedKey;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.client.sdk.query.PreparedAel;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.recordversioning.model.TradeBase;
import com.aerospike.mapper.tools.AeroMapper;

/**
 * SDK port of the legacy {@code DeltaVersioningRecords} (see ../../java). Same {@link TradeBase}
 * live-record-plus-{@code versions}-map model as {@link VersioningRecords}, but instead of copying
 * the entire prior record to a historical key on every change, each update writes a small
 * <em>delta</em> audit record ({@code id:version}) describing only which bins changed and how
 * ({@code Inserted}/{@code Changed}/{@code Same}), plus their new values - enough to reconstruct any
 * past version by replaying deltas from version 0 forward.
 * <p/>
 * Diffing happens server-side: each touched bin gets a paired {@code when(...)} AEL expression
 * (read-only, via {@code selectFrom}) alongside its {@code setTo} write in the same {@code operate}
 * call, so only the small classification label - not the bin's prior value - ever crosses the wire;
 * the "new" value doesn't need to travel back at all since the caller already supplied it as
 * {@code newValues}. Status labels use positional keys ({@code s0}, {@code s1}, ...) rather than a
 * per-bin-name label, since a real bin name plus a suffix can easily exceed Aerospike's 15-character
 * bin name limit.
 * <p/>
 * One piece is still a targeted follow-up call rather than folded into the same round trip: closing
 * the {@code versions} map's entry currently marked -1 and opening a new one needs a key discovered
 * at runtime via a value selector ({@code $.versions.{=-1,}.getKeys()}), and selector operands /
 * collection-literal keys must be static literals (AEL_CANONICAL_REFERENCE.md §4.2, §5) - a
 * discovered key can't then address a write in that same expression. That selector also throws
 * (rather than yielding an empty match) when the {@code versions} bin doesn't exist yet, which is
 * exactly the very-first-delta-for-this-id case - handled by retrying the same diff call without it
 * (there's no prior -1 entry to discover anyway).
 */
public class DeltaVersioningRecords implements UseCase {

    private static final int NUM_RECORDS = 100;
    private static final Set<String> PROTECTED_BINS = Set.of("version", "versions");

    private static final String[] SOURCE_SYSTEMS = {"FIX", "MANUAL", "BLOOMBERG", "REUTERS"};
    private static final String[] BOOKS = {"EQ-DESK-1", "FX-DESK-2", "RATES-DESK-3", "CREDIT-DESK-4"};
    private static final String[] COUNTERPARTIES = {"Acme Capital", "Northwind Bank", "Contoso Securities", "Fabrikam Trading"};

    @Override
    public String getName() {
        return "Delta Versioning Records";
    }

    @Override
    public String getDescription() {
        return "Maintain an audit trail of TradeBase record changes using delta records instead of full copies. "
                + "The initial insert is recorded as delta version 0 with all bins marked Inserted. Subsequent "
                + "changes store bin-level deltas (Inserted, Changed, Removed), including new values, so any past "
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
            // ".type" is mapper-injected type-discriminator metadata, not a real trade field - a
            // bin name containing "." can't be embedded in an AEL path (see statusAel), and
            // diffing/versioning it wouldn't be meaningful even if it could be.
            initial.remove(".type");
            updateTradeBaseWithDelta(session, id, System.currentTimeMillis(), "Initial insert", "setup", initial);
        }
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

    @SuppressWarnings("unchecked")
    private ChainableOperationBuilder setBinFromValue(ChainableOperationBuilder builder, String name, Object value) {
        var bin = builder.bin(name);
        if (value == null) {
            return bin.setTo("");
        }
        else if (value instanceof Long l) {
            return bin.setTo((long) l);
        }
        else if (value instanceof Integer i) {
            return bin.setTo((long) i);
        }
        else if (value instanceof String s) {
            return bin.setTo(s);
        }
        else if (value instanceof Boolean b) {
            return bin.setTo((boolean) b);
        }
        else if (value instanceof Double d) {
            return bin.setTo((double) d);
        }
        else if (value instanceof byte[] by) {
            return bin.setTo(by);
        }
        else if (value instanceof List<?> li) {
            return bin.setTo(li);
        }
        else if (value instanceof Map<?, ?> m) {
            return bin.setTo((Map<Object, Object>) m);
        }
        throw new IllegalStateException("Unsupported bin value type for '" + name + "': " + value.getClass());
    }

    /** A {@code when(...)} AEL expression classifying a bin's prior state against its new value. */
    private static String statusAel(String binName, Object newValue) {
        String template = "when($." + binName + ".exists() == false => 'Inserted', "
                + "$." + binName + " == ?0 => 'Same', default => 'Changed')";
        return PreparedAel.prepare(template).formValue(newValue);
    }

    /**
     * Builds the diff-and-write {@code operate} call: each touched bin gets a positionally-keyed
     * {@code selectFrom(statusAel(...))} read alongside its {@code setTo} write, plus a read of the
     * live {@code version} (or -1 if the record is new), and - unless {@code includeCurKey} is
     * false - the {@code versions} map's current -1-marked key. See the class javadoc for why
     * {@code includeCurKey} sometimes has to be false.
     */
    private ChainableOperationBuilder buildDiffOp(Session tx, TypedKey<TradeBase> key, List<String> binOrder,
            Map<String, Object> newValues, boolean includeCurKey) {
        ChainableOperationBuilder op = tx.upsert(key);
        for (int i = 0; i < binOrder.size(); i++) {
            String binName = binOrder.get(i);
            Object newValue = newValues.get(binName);
            op = op.bin("s" + i).selectFrom(statusAel(binName, newValue));
            op = setBinFromValue(op, binName, newValue);
        }
        op = op.bin("version$old").selectFrom("when($.version.exists() == false => -1, default => $.version)");
        if (includeCurKey) {
            op = op.bin("curKey").selectFrom("$.versions.{=-1,}.getKeys()");
        }
        return op;
    }

    /**
     * Applies {@code newValues} to the live {@link TradeBase} record at key {@code id}, computes
     * the bin-level delta against its prior state, and writes both the updated live record and a
     * new immutable delta audit record at {@code id:newVersion} - all inside one transaction.
     *
     * @return the new version number
     */
    public int updateTradeBaseWithDelta(Session session, long id, long timestamp, String description, String user,
            Map<String, Object> newValues) {
        return session.doInTransactionReturning(tx -> {
            TypedKey<TradeBase> key = formKey(id);

            List<String> binOrder = newValues.keySet().stream()
                    .filter(binName -> !PROTECTED_BINS.contains(binName))
                    .toList();

            Record diffResult;
            boolean includeCurKey = true;
            try {
                diffResult = buildDiffOp(tx, key, binOrder, newValues, true).execute().getFirstRecord();
            }
            catch (AerospikeException.BinOpInvalidException e) {
                includeCurKey = false;
                diffResult = buildDiffOp(tx, key, binOrder, newValues, false).execute().getFirstRecord();
            }

            long currentVersion = diffResult.getLong("version$old");
            List<?> curVersionKey = includeCurKey ? diffResult.getList("curKey") : null;

            List<Map<String, Object>> changes = new ArrayList<>();
            for (int i = 0; i < binOrder.size(); i++) {
                String binName = binOrder.get(i);
                String status = diffResult.getString("s" + i);
                if (!"Same".equals(status)) {
                    Map<String, Object> change = new LinkedHashMap<>();
                    change.put("binName", binName);
                    change.put("status", status);
                    change.put("newValue", newValues.get(binName));
                    changes.add(change);
                }
            }

            long changeTs = timestamp == 0 ? System.currentTimeMillis() : timestamp;
            int newVersion = (int) currentVersion + 1;
            ChainableOperationBuilder op = tx.upsert(key)
                    .bin("version").setTo((long) newVersion)
                    .bin("updatedDate").setTo(changeTs);
            if (curVersionKey != null && !curVersionKey.isEmpty()) {
                long mapKeyOfCurrentVersion = ((Number) curVersionKey.get(0)).longValue();
                op = op.bin("versions").onMapKey(mapKeyOfCurrentVersion, MapOrder.KEY_ORDERED).upsert(currentVersion)
                        .bin("versions").onMapKey(changeTs, MapOrder.KEY_ORDERED).upsert(-1L);
            }
            else {
                op = op.bin("versions").onMapKey(changeTs, MapOrder.KEY_ORDERED).upsert(-1L);
            }
            op.execute();

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

        Record rec = session.query(formKey(tradeId)).execute().getFirstRecord();
        int currentTradeVersion = rec.getInt("tradeVersion");
        updateTradeBaseWithDelta(session, tradeId, 0, "Increment trade version", "batch-user",
                Map.of("tradeVersion", (long) (currentTradeVersion + 1)));

        updateTradeBaseWithDelta(session, tradeId, 0, "Update counterparty and book", "alice",
                Map.of("counterparty", "CP-1001", "book", "XY"));

        updateTradeBaseWithDelta(session, tradeId, 0, "Mark record complete", "bob",
                Map.of("recordComplete", true));

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
