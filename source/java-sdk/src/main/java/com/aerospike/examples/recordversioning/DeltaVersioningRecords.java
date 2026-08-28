package com.aerospike.examples.recordversioning;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import com.aerospike.client.sdk.ChainableOperationBuilder;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordMapper;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.TypedDataSet;
import com.aerospike.client.sdk.TypedKey;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.recordversioning.model.TradeBase;

/**
 * SDK port of the legacy {@code DeltaVersioningRecords} (see ../../java). Same {@link TradeBase}
 * live-record-plus-{@code versions}-map model as {@link VersioningRecords}, but instead of copying
 * the entire prior record to a historical key on every change, each update writes a small
 * <em>delta</em> audit record ({@code id:version}) describing only which bins changed and how
 * ({@code Inserted}/{@code Changed}/{@code Removed}), plus their new values - enough to reconstruct
 * any past version by replaying deltas from version 0 forward.
 * <p/>
 * The legacy version detects these deltas entirely server-side: it snapshots each targeted bin
 * into a temporary bin, applies the caller's operations, then compares before/after using nested
 * conditional write expressions ({@code Exp.cond} combined with {@code MapExp.put}/{@code
 * ListExp.getByIndex}/{@code MapExp.getByValue}, several levels deep). Given the nested-expression
 * composition gaps already found elsewhere in this port against this alpha SDK build - and that
 * this use case's expressions nest considerably deeper than any of those - this port computes the
 * before/after diff client-side instead: read the live record, compare each caller-supplied bin
 * value against what was there before, and only write bins that actually changed. Same observable
 * behavior (audit trail, point-in-time versions map, reconstruction), simpler and more round trips
 * instead of one expression-heavy operate call.
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

    private final TypedDataSet<TradeBase> tradeBases =
            TypedDataSet.of(System.getProperty("demo.namespace", "test"), "uccb_tradebase", TradeBase.class);

    private TypedKey<TradeBase> formKey(long id) {
        return tradeBases.id(id);
    }

    private TypedKey<TradeBase> formKey(long id, int version) {
        return tradeBases.id(id + ":" + version);
    }

    @Override
    public void setup(Session session) throws Exception {
        session.truncate(tradeBases);

        System.out.printf("Generating %,d trades%n", NUM_RECORDS);
        RecordMapper<TradeBase> mapper = session.getRecordMappingFactory().getMapper(TradeBase.class);
        for (long id = 0; id < NUM_RECORDS; id++) {
            TradeBase trade = randomTradeBase(id);
            Map<String, Object> initial = mapper.toMap(trade);
            initial.remove("version");
            initial.remove("versions");
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

    /**
     * Applies {@code newValues} to the live {@link TradeBase} record at key {@code id}, computes
     * the bin-level delta against its prior state, and writes both the updated live record and a
     * new immutable delta audit record at {@code id:newVersion} - all inside one transaction.
     *
     * @return the new version number
     */
    @SuppressWarnings("unchecked")
    public int updateTradeBaseWithDelta(Session session, long id, long timestamp, String description, String user,
            Map<String, Object> newValues) {
        return session.doInTransactionReturning(tx -> {
            TypedKey<TradeBase> key = formKey(id);
            Record existing = tx.query(key).execute().getFirstRecord();

            Map<String, Object> before = new HashMap<>();
            int currentVersion = -1;
            Map<Long, Long> versions = null;
            if (existing != null) {
                before.putAll(existing.bins);
                currentVersion = existing.getInt("version");
                versions = (Map<Long, Long>) existing.getMap("versions");
            }

            long changeTs = timestamp == 0 ? System.currentTimeMillis() : timestamp;
            List<Map<String, Object>> changes = new ArrayList<>();
            ChainableOperationBuilder op = tx.upsert(key);
            for (Map.Entry<String, Object> entry : newValues.entrySet()) {
                String binName = entry.getKey();
                if (PROTECTED_BINS.contains(binName)) {
                    continue;
                }
                Object newValue = entry.getValue();
                String status = !before.containsKey(binName) ? "Inserted"
                        : Objects.equals(before.get(binName), newValue) ? "Same" : "Changed";
                if (!"Same".equals(status)) {
                    Map<String, Object> change = new LinkedHashMap<>();
                    change.put("binName", binName);
                    change.put("status", status);
                    change.put("newValue", newValue);
                    changes.add(change);
                    op = setBinFromValue(op, binName, newValue);
                }
            }

            int newVersion = currentVersion + 1;
            op = op.bin("version").setTo((long) newVersion).bin("updatedDate").setTo(changeTs);
            if (versions != null && !versions.isEmpty()) {
                long mapKeyOfCurrentVersion = versions.entrySet().stream()
                        .filter(e -> e.getValue() == -1)
                        .map(Map.Entry::getKey)
                        .findFirst()
                        .orElseThrow();
                op = op.bin("versions").onMapKey(mapKeyOfCurrentVersion, MapOrder.KEY_ORDERED).upsert((long) currentVersion)
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

    /** Returns all delta records for a trade, from version 0 through its current live version. */
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

    /** Reconstructs the bin values of a trade as they existed at {@code targetVersion} by replaying deltas. */
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
    public void run(Session session) throws Exception {
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
