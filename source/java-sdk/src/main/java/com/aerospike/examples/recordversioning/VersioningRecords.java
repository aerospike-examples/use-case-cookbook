package com.aerospike.examples.recordversioning;

import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

import com.aerospike.client.sdk.ChainableOperationBuilder;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.TypedDataSet;
import com.aerospike.client.sdk.TypedKey;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.examples.Async;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.recordversioning.model.TradeBase;
import com.aerospike.examples.recordversioning.model.TradeStaticData;

/**
 * SDK port of the legacy {@code VersioningRecords} (see ../../java). Maintains historical
 * versions of records: the "effective" record (unversioned key, e.g. {@code trade:12345}) always
 * reflects the latest data, and each update first copies the current effective record verbatim
 * (except its {@code versions} bin) to a new immutable historical record keyed {@code
 * trade:12345:N} before applying the change - all inside one transaction. The effective record's
 * {@code versions} bin is a key-ordered map from update timestamp to the version number that was
 * effective starting at that time (the newest entry is always version {@code -1}, meaning "look
 * at the effective record itself, not a historical one").
 * <p/>
 * Each caller-supplied {@link ChangeHandler} receives the record as read inside the transaction
 * and the {@link ChainableOperationBuilder} already targeting the effective key, and returns that
 * builder extended with its own bin changes chained on - {@code version}/{@code updatedDate}/{@code
 * versions} are then chained on top of whatever the handler added.
 */
public class VersioningRecords implements UseCase {

    public interface ChangeHandler {
        ChainableOperationBuilder apply(Record existingItem, ChainableOperationBuilder builder);
    }

    private static final int NUM_TRADES = 10_000;
    private static final String[] SOURCE_SYSTEMS = {"FIX", "MANUAL", "BLOOMBERG", "REUTERS"};
    private static final String[] BOOKS = {"EQ-DESK-1", "FX-DESK-2", "RATES-DESK-3", "CREDIT-DESK-4"};
    private static final String[] COUNTERPARTIES = {"Acme Capital", "Northwind Bank", "Contoso Securities", "Fabrikam Trading"};

    @Override
    public String getName() {
        return "Versioned Records";
    }

    @Override
    public String getDescription() {
        return "Maintain historical versions of records with point-in-time query capabilities. Demonstrates atomic "
                + "version creation using transactions and time-based queries using map operations. Objects are assumed "
                + "to have 2 parts -- a base record which changes frequently and is small, and a details record which is "
                + "large and changes infrequently.";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/versioning-records.md";
    }

    private final TypedDataSet<TradeBase> tradeBases =
            TypedDataSet.of(System.getProperty("demo.namespace", "test"), "uccb_tradebase", TradeBase.class);
    private final TypedDataSet<TradeStaticData> tradeContents =
            TypedDataSet.of(System.getProperty("demo.namespace", "test"), "uccb_tradecontent", TradeStaticData.class);

    private <T> TypedKey<T> formKey(TypedDataSet<T> dataSet, long id) {
        return dataSet.id(id);
    }

    private <T> TypedKey<T> formKey(TypedDataSet<T> dataSet, long id, int version) {
        return version < 0 ? formKey(dataSet, id) : dataSet.id(id + ":" + version);
    }

    @Override
    public void setup(Session session) throws Exception {
        session.truncate(tradeBases);
        session.truncate(tradeContents);

        System.out.printf("Generating %,d trades%n", NUM_TRADES);
        Date now = new Date();
        for (long id = 0; id < NUM_TRADES; id++) {
            TradeStaticData content = randomTradeContent(id);
            TradeBase trade = randomTradeBase(id);
            trade.setVersion(0);
            trade.setDataVersion(content.getVersion());
            // The current version is always -1
            trade.setVersions(Map.of(now.getTime(), -1L));
            trade.setUpdatedDate(now);

            session.upsert(tradeContents).object(content).execute();
            session.upsert(tradeBases).object(trade).execute();
        }
    }

    private TradeStaticData randomTradeContent(long tradeId) {
        TradeStaticData data = new TradeStaticData();
        data.setTradeId(tradeId);
        data.setVersion(0);
        data.setMutableData(0);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            sb.append("word").append(ThreadLocalRandom.current().nextInt(1000)).append(' ');
        }
        data.setData(sb.toString());
        return data;
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
        trade.setRecordComplete(true);
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
     * Updates the effective record for {@code id}, first copying it to a new historical version.
     * All within one transaction.
     *
     * @return the new version number
     */
    private <T> int updateObjectWithVersion(Session session, TypedDataSet<T> dataSet, long id, long timestamp, ChangeHandler handler) {
        return session.doInTransactionReturning(tx -> {
            TypedKey<T> unversionedKey = formKey(dataSet, id);
            Record rec = tx.query(unversionedKey).execute().getFirstRecord();

            int currentVersion = rec.getInt("version");
            Object versionsBin = rec.getValue("versions");

            // Copy the current effective record (except "versions") to a new historical, immutable record.
            TypedKey<T> versionedKey = formKey(dataSet, id, currentVersion);
            ChainableOperationBuilder copyOp = tx.insert(versionedKey);
            for (Map.Entry<String, Object> entry : rec.bins.entrySet()) {
                if (!"versions".equals(entry.getKey())) {
                    copyOp = setBinFromValue(copyOp, entry.getKey(), entry.getValue());
                }
            }
            copyOp.execute();

            // Apply the caller's changes, then bump the version/updatedDate/versions map on the effective record.
            int newVersion = currentVersion + 1;
            long tsToUse = timestamp == 0 ? new Date().getTime() : timestamp;

            ChainableOperationBuilder updateOp = handler.apply(rec, tx.update(unversionedKey));
            updateOp = updateOp.bin("version").setTo((long) newVersion).bin("updatedDate").setTo(tsToUse);

            if (versionsBin != null) {
                @SuppressWarnings("unchecked")
                Map<Long, Long> versions = (Map<Long, Long>) versionsBin;
                long mapKeyOfCurrentVersion = versions.entrySet().stream()
                        .filter(e -> e.getValue() == -1)
                        .map(Map.Entry::getKey)
                        .findFirst()
                        .orElseThrow(() -> new NoSuchElementException("No value mapped to -1"));

                updateOp = updateOp.bin("versions").onMapKey(mapKeyOfCurrentVersion, MapOrder.KEY_ORDERED).upsert(currentVersion)
                        .bin("versions").onMapKey(tsToUse, MapOrder.KEY_ORDERED).upsert(-1L);
            }
            updateOp.execute();

            return newVersion;
        });
    }

    /**
     * Reads a TradeBase record as it existed at a specific point in time. Uses {@code
     * onMapKeyRelativeIndexRange} to find the version active at or before {@code timestamp} in one
     * call: search for the first {@code versions} map key strictly greater than {@code
     * timestamp + 1}, then step back one entry.
     */
    private TradeBase readAtTime(Session session, long id, long timestamp) {
        TypedKey<TradeBase> unversionedKey = formKey(tradeBases, id);

        // The read-side query builder doesn't expose onMapKeyRelativeIndexRange (only the write-side
        // one does) - go through upsert() purely as a vehicle for that CDT sub-op. No bin is set, so
        // nothing is actually written; the record is guaranteed to already exist by this point.
        Record rec = session.upsert(unversionedKey)
                .bin("versions").onMapKeyRelativeIndexRange(timestamp + 1, -1, 1).getKeysAndValues()
                .execute().getFirstRecord();
        if (rec == null) {
            throw new IllegalArgumentException("No trade base with id: " + id);
        }
        @SuppressWarnings("unchecked")
        Map<Long, Long> versionsResult = (Map<Long, Long>) rec.getMap("versions");
        if (versionsResult == null || versionsResult.isEmpty()) {
            // Before the earliest recorded version
            return null;
        }
        long version = versionsResult.values().iterator().next();

        if (version == -1) {
            return session.query(unversionedKey).execute().getFirstObject().orElseThrow();
        }
        else {
            TypedKey<TradeBase> versionedKey = formKey(tradeBases, id, (int) version);
            return session.query(versionedKey).execute().getFirstObject().orElseThrow();
        }
    }

    @Override
    public void run(Session session) throws Exception {
        Async.runFor(Duration.ofSeconds(5), async -> {
            async.periodic(Duration.ofMillis(200), () -> {
                long now = System.nanoTime();
                final long tradeId = 2;
                int newVersion = updateObjectWithVersion(session, tradeContents, tradeId, 0, (rec, builder) ->
                        builder.bin("mutableData").add(3));
                updateObjectWithVersion(session, tradeBases, tradeId, 0, (rec, builder) ->
                        builder.bin("dataVersion").add(newVersion));
                System.out.printf("Update took %,dus%n", (System.nanoTime() - now) / 1_000);
            });

            async.periodic(Duration.ofSeconds(1), () ->
                    updateObjectWithVersion(session, tradeBases, 1, 0, (rec, builder) ->
                            builder.bin("tradeVersion").add(3)));
        });

        long now = new Date().getTime();
        System.out.println("Version map:");
        Record versionsRecord = session.query(formKey(tradeBases, 2)).readingOnlyBins("versions").execute().getFirstRecord();
        System.out.println(versionsRecord == null ? null : versionsRecord.getMap("versions"));

        System.out.printf("%nReading current version (at %d):", now);
        System.out.println(readAtTime(session, 2, now));
        System.out.println("\nReading current version 2 seconds ago:");
        System.out.println(readAtTime(session, 2, now - 2000));
        System.out.println("\nReading before the first version");
        System.out.println(readAtTime(session, 2, now - 200_000));
    }
}
