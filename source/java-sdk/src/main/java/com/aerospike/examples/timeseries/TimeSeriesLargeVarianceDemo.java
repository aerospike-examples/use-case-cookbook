package com.aerospike.examples.timeseries;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.cdt.ListOrder;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.examples.AnsiColors;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.timeseries.model.Account;
import com.aerospike.examples.timeseries.model.Event;

/**
 * SDK port of the legacy {@code TimeSeriesLargeVarianceDemo} (see ../../java). Same bucketed
 * time-series model as {@link TimeSeriesDemo}, but each bucket record adaptively splits once it
 * holds more than {@code MAX_RECORDS_PER_BUCKET} events: the oldest {@code
 * PERCENT_EVENTS_IN_ORIG_BUCKET}% stay in the original ("root") record, and the rest move to a new
 * continuation sub-record named after the lowest eventId it holds. The root record's {@code cont}
 * bin holds an ascending list of these split-point eventIds, so a query walks that list to find
 * which sub-record(s) it needs, and a write walks it to find which sub-record a new event belongs
 * in (recursing into that sub-record's own split if it, in turn, overflows).
 * <p/>
 * {@link #splitBucket} detects overflow and removes the minority slice in one {@code operate()}
 * call against the bucket record. The split point ({@link #MINOR_SPLIT_ITEMS}) is a fixed item
 * count rather than a percentage of the bucket's current size, because AEL selector bounds must
 * be static literals - a computed bound like {@code count() * 80 / 100} isn't allowed inside
 * {@code {…}} (AEL_CANONICAL_REFERENCE.md §4.2).
 */
public class TimeSeriesLargeVarianceDemo implements UseCase {

    public enum SortDirection {
        ASCENDING,
        DESCENDING
    }

    private static final String BIN_NAME = "map";
    private static final String CONTINUATION_BIN = "cont";
    private static final int MAX_RECORDS_PER_BUCKET = 10;
    private static final int PERCENT_EVENTS_IN_ORIG_BUCKET = 80;
    private static final int MINOR_SPLIT_ITEMS = MAX_RECORDS_PER_BUCKET * (100 - PERCENT_EVENTS_IN_ORIG_BUCKET) / 100;

    private static final long MILLIS_PER_HOUR = TimeUnit.HOURS.toMillis(1);
    private static final long DATE_OFFSET_MILLIS = TimeUnit.DAYS.toMillis(LocalDate.of(2024, 1, 1).toEpochDay());
    private static final int BUCKET_WIDTH_HOURS = 24;
    private static final int MAX_DAYS_TO_STORE = 14;
    private static final int HOURS_PER_DAY = 24;
    private static final int EVENT_ID_TIMESTAMP_LENGTH = 13;

    private static final int NUM_ACCOUNTS = 100;
    private static final int MAX_EVENTS_PER_DEVICE = 20;
    private static final int NUM_EVENTS_ACCT_1 = 25_000;
    private static final String DEFAULT_VIDEO_URL = "https://somewhere.com/4659278373492";
    private static final String DEFAULT_STORAGE_LOCATION = "hv";

    private static volatile boolean expirationWarningShown = false;

    @Override
    public String getName() {
        return "Time-series data with large variation";
    }

    @Override
    public String getDescription() {
        return "Demonstrates how to store, update and query time-series data when there can be a large disparity "
                + "in the events for devices. This is applicable to many ad-hoc time series events like identifying fraud in "
                + "credit card swipes. (Consumers might do 20 a day, businesses could do 100,000). In this case the data is "
                + "devices which generate events. These devices could be motion sensors, cameras, etc. "
                + "The data model has many accounts, each account has a handful of devices, and the devices "
                + "generate events when triggered. The events are stored for 14 days, and queries can be "
                + "performed on the events for an account, filtering by time range and / or a list of device ids. "
                + "This shows a way to store time series data with events occurring on a sporadic (random) basis with "
                + "high variability in cardinality.";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/timeseries-large-variance.md";
    }

    private final DataSet events = DataSet.of(System.getProperty("demo.namespace", "test"), "uccb_events_variance");

    @Override
    public void setup(Session session) throws Exception {
        session.truncate(events);
        generateSampleData(session);
    }

    @Override
    public void run(Session session) throws Exception {
        demonstrateQueries(session);
    }

    // ------------------------------------------------------------------
    // Bucket / event-id helpers (identical logic to TimeSeriesDemo)
    // ------------------------------------------------------------------

    public static long getBucketOffset(long timestamp) {
        return (timestamp - DATE_OFFSET_MILLIS) / (MILLIS_PER_HOUR * BUCKET_WIDTH_HOURS);
    }

    private Key createEventKey(String accountId, long timestamp) {
        return events.id(accountId + ":" + getBucketOffset(timestamp));
    }

    private Key getContinuationKeyFromKey(Key key, String subKey) {
        return events.id(key.userKey.toString() + "-" + subKey);
    }

    private static long dateToLong(Date date) {
        return date == null ? 0 : date.getTime();
    }

    private static Date longToDate(long timestamp) {
        return timestamp == 0 ? null : new Date(timestamp);
    }

    private static long extractTimestampFromEventId(String eventId) {
        return Long.parseLong(eventId.substring(0, EVENT_ID_TIMESTAMP_LENGTH));
    }

    private static String eventIdFromTimestamp(long timestamp, boolean lowerBound) {
        return lowerBound
                ? String.format("%013d%012d", timestamp, 0)
                : String.format("%013d%d", timestamp, 999_999_999_999L);
    }

    private static String generateNextEventId(String eventId) {
        long value = Long.parseLong(eventId.substring(EVENT_ID_TIMESTAMP_LENGTH));
        return String.format("%s%012d", eventId.substring(0, EVENT_ID_TIMESTAMP_LENGTH), value + 1);
    }

    // ------------------------------------------------------------------
    // Event <-> Map conversion
    // ------------------------------------------------------------------

    private static Map<String, Object> convertEventToMap(Event event) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", event.getId());
        map.put("accountId", event.getAccountId());
        map.put("deviceId", event.getDeviceId());
        map.put("params", event.getParameters());
        map.put("resolution", event.getResolution());
        map.put("videoMeta", event.getVideoMeta());
        map.put("paramTags", event.getParameterTags());
        map.put("partnerId", event.getPartnerId());
        map.put("partStateId", event.getPartnerStateId());
        map.put("timestamp", dateToLong(event.getTimestamp()));
        return map;
    }

    @SuppressWarnings("unchecked")
    private static Event convertMapToEvent(Map<String, Object> eventMap) {
        if (eventMap == null) {
            return null;
        }
        Event event = new Event();
        event.setId((String) eventMap.get("id"));
        event.setAccountId((String) eventMap.get("accountId"));
        event.setDeviceId((String) eventMap.get("deviceId"));
        event.setParameters((Map<String, Object>) eventMap.get("params"));
        event.setResolution((List<Integer>) eventMap.get("resolution"));
        event.setVideoMeta((Map<String, Object>) eventMap.get("videoMeta"));
        event.setParameterTags((List<String>) eventMap.get("paramTags"));
        event.setPartnerId((String) eventMap.get("partnerId"));
        event.setPartnerStateId((String) eventMap.get("partStateId"));
        event.setTimestamp(longToDate((Long) eventMap.get("timestamp")));
        return event;
    }

    // ------------------------------------------------------------------
    // Writes: insert with adaptive bucket splitting
    // ------------------------------------------------------------------

    public void upsertEvent(Session session, Event event, boolean setExpiry) {
        validateEvent(event);
        Key rootKey = createEventKey(event.getAccountId(), event.getTimestamp().getTime());

        session.doInTransaction(tx -> {
            List<String> continuation = readContinuationList(tx, rootKey);
            Key targetKey = rootKey;
            for (int i = continuation.size() - 1; i >= 0; i--) {
                if (continuation.get(i).compareTo(event.getId()) <= 0) {
                    targetKey = getContinuationKeyFromKey(rootKey, continuation.get(i));
                    break;
                }
            }

            long size = writeEventAndGetBucketSize(tx, targetKey, event, setExpiry);
            if (size > MAX_RECORDS_PER_BUCKET) {
                splitBucket(tx, rootKey, targetKey);
            }
        });
    }

    private List<String> readContinuationList(Session session, Key rootKey) {
        Record record = session.query(rootKey).readingOnlyBins(CONTINUATION_BIN).execute().getFirstRecord();
        if (record == null) {
            return List.of();
        }
        List<?> list = record.getList(CONTINUATION_BIN);
        return list == null ? List.of() : list.stream().map(item -> (String) item).toList();
    }

    private long writeEventAndGetBucketSize(Session session, Key key, Event event, boolean setExpiry) {
        try {
            var op = session.upsert(key);
            op = setExpiry ? op.expireRecordAfter(java.time.Duration.ofDays(MAX_DAYS_TO_STORE)) : op.withNoChangeInExpiration();
            op.bin(BIN_NAME).onMapKey(event.getId(), MapOrder.KEY_ORDERED)
                    .upsert(List.of(event.getDeviceId(), convertEventToMap(event)))
                    .execute();

            // Deliberately a separate call from the write above - requesting a write and a read of
            // the same bin in one operation returns a multi-result wrapper instead of a plain value
            // (see the equivalent note in PlayerMatching).
            Record record = session.upsert(key).bin(BIN_NAME).mapSize().execute().getFirstRecord();
            return record.getLong(BIN_NAME);
        }
        catch (AerospikeException ae) {
            // Same eviction-disabled namespace gotcha as TimeSeriesDemo - see there for details.
            if (setExpiry && ae.getResultCode() == ResultCode.FAIL_FORBIDDEN) {
                if (!expirationWarningShown) {
                    expirationWarningShown = true;
                    System.out.println(AnsiColors.YELLOW
                            + "Note: this namespace does not support record expiration (eviction is disabled) - "
                            + "events will be written without a TTL." + AnsiColors.RESET);
                }
                return writeEventAndGetBucketSize(session, key, event, false);
            }
            throw ae;
        }
    }

    /**
     * Splits an overflowing bucket: conditionally (only when the bucket's current size is still
     * {@code >= MAX_RECORDS_PER_BUCKET}) reads and removes its newest {@link #MINOR_SPLIT_ITEMS}
     * events in one atomic {@code operate()} call against the bucket record, then - if anything
     * was removed - persists that minority slice to a new continuation sub-record and records the
     * split point on the root record's (ascending, ordered) {@code cont} list.
     * <p/>
     * The {@code when(...)} condition re-checks the size rather than trusting the caller, because
     * {@link #upsertEvent} calls this right after a separate write that may have raced with
     * another writer.
     */
    @SuppressWarnings("unchecked")
    private void splitBucket(Session session, Key rootKey, Key bucketKey) {
        String minorityAel = String.format(
                "when ($.%s:MAP.count() >= %d => $.%s.{-%d:}.getMaps(), default => {})",
                BIN_NAME, MAX_RECORDS_PER_BUCKET, BIN_NAME, MINOR_SPLIT_ITEMS);
        String majorityAel = String.format(
                "when ($.%s:MAP.count() >= %d => $.%s.{-%d:}.remove(), default => $.%s.{0:}.getMaps())",
                BIN_NAME, MAX_RECORDS_PER_BUCKET, BIN_NAME, MINOR_SPLIT_ITEMS, BIN_NAME);

        Record result = session.upsert(bucketKey)
                .bin("minorityOut").selectFrom(minorityAel)
                .bin(BIN_NAME).upsertFrom(majorityAel)
                .execute().getFirstRecord();

        Map<String, Object> minorityMap = (Map<String, Object>) result.getMap("minorityOut");
        if (minorityMap == null || minorityMap.isEmpty()) {
            return;
        }
        String splitPointEventId = Collections.min(minorityMap.keySet());
        Key minorityKey = getContinuationKeyFromKey(rootKey, splitPointEventId);

        session.upsert(minorityKey).bin(BIN_NAME).setTo(minorityMap).execute();
        session.upsert(rootKey)
                .bin(CONTINUATION_BIN).listCreate(ListOrder.ORDERED)
                .bin(CONTINUATION_BIN).listAppend(splitPointEventId, opts -> opts.addUnique().allowFailures())
                .execute();
    }

    // ------------------------------------------------------------------
    // Reads
    // ------------------------------------------------------------------

    private long getOldestTimestamp(Long startTimestamp) {
        return startTimestamp == null ? new Date().getTime() - TimeUnit.DAYS.toMillis(MAX_DAYS_TO_STORE) : startTimestamp;
    }

    private long getLatestTimestamp(Long endTimestamp) {
        return endTimestamp == null ? new Date().getTime() : endTimestamp;
    }

    public List<Event> getEventsBetween(Session session, String accountId, Long startTimestamp, Long endTimestamp,
            String eventId, int count, SortDirection direction, String... deviceIds) {
        validateAccountId(accountId);
        validateCount(count);
        validateTimestamps(startTimestamp, endTimestamp);

        List<Event> results = new ArrayList<>();

        String latestEventId;
        String earliestEventId;
        if (eventId != null) {
            if (direction == SortDirection.ASCENDING) {
                earliestEventId = generateNextEventId(eventId);
                latestEventId = eventIdFromTimestamp(getLatestTimestamp(endTimestamp), false);
            }
            else {
                latestEventId = eventId;
                earliestEventId = eventIdFromTimestamp(getOldestTimestamp(startTimestamp), true);
            }
        }
        else {
            latestEventId = eventIdFromTimestamp(getLatestTimestamp(endTimestamp), false);
            earliestEventId = eventIdFromTimestamp(getOldestTimestamp(startTimestamp), true);
        }

        long startRecord = getBucketOffset(extractTimestampFromEventId(earliestEventId));
        long endRecord = getBucketOffset(extractTimestampFromEventId(latestEventId));
        java.util.Set<String> deviceFilter = deviceIds.length == 0 ? null : java.util.Set.of(deviceIds);

        if (direction == SortDirection.ASCENDING) {
            for (long recordKey = startRecord; results.size() < count && recordKey <= endRecord; recordKey++) {
                Key key = events.id(accountId + ":" + recordKey);
                processRootAndContinuations(session, key, earliestEventId, latestEventId, count, results, direction, deviceFilter);
            }
        }
        else {
            for (long recordKey = endRecord; results.size() < count && recordKey >= startRecord; recordKey--) {
                Key key = events.id(accountId + ":" + recordKey);
                processRootAndContinuations(session, key, earliestEventId, latestEventId, count, results, direction, deviceFilter);
            }
        }

        return results;
    }

    private void processRootAndContinuations(Session session, Key rootKey, String earliestEventId, String latestEventId,
            int count, List<Event> results, SortDirection direction, java.util.Set<String> deviceFilter) {
        Record record = session.query(rootKey).execute().getFirstRecord();
        if (record == null) {
            return;
        }
        List<?> continuationBin = record.getList(CONTINUATION_BIN);

        addEventsToResults(count, readFilteredBucket(session, rootKey, earliestEventId, latestEventId, deviceFilter), results, direction);

        if (continuationBin != null && !continuationBin.isEmpty()) {
            @SuppressWarnings("unchecked")
            List<String> continuation = (List<String>) continuationBin;
            if (direction == SortDirection.ASCENDING) {
                for (String subKey : continuation) {
                    if (subKey.compareTo(latestEventId) > 0 || results.size() >= count) {
                        break;
                    }
                    Key subRecordKey = getContinuationKeyFromKey(rootKey, subKey);
                    addEventsToResults(count, readFilteredBucket(session, subRecordKey, earliestEventId, latestEventId, deviceFilter), results, direction);
                }
            }
            else {
                for (int i = continuation.size() - 1; i >= 0; i--) {
                    String subKey = continuation.get(i);
                    Key subRecordKey = getContinuationKeyFromKey(rootKey, subKey);
                    addEventsToResults(count, readFilteredBucket(session, subRecordKey, earliestEventId, latestEventId, deviceFilter), results, direction);
                    if (subKey.compareTo(earliestEventId) < 0 || results.size() >= count) {
                        break;
                    }
                }
            }
        }
    }

    public List<Event> getEventsBefore(Session session, String accountId, String eventId, int count, String... deviceIds) {
        return getEventsBetween(session, accountId, null, null, eventId, count, SortDirection.DESCENDING, deviceIds);
    }

    public List<Event> getEventsAfter(Session session, String accountId, String eventId, int count, String... deviceIds) {
        return getEventsBetween(session, accountId, null, null, eventId, count, SortDirection.ASCENDING, deviceIds);
    }

    /**
     * Reads a bucket's events restricted to an eventId key range, with device filtering (when
     * requested) also pushed down server-side. See {@link TimeSeriesDemo#readFilteredBucket} for
     * the AEL selector-plus-filter derivation.
     */
    private Record readFilteredBucket(Session session, Key key, String earliestEventId, String latestEventId,
            java.util.Set<String> deviceFilter) {
        StringBuilder ael = new StringBuilder()
                .append("$.").append(BIN_NAME)
                .append(".{@'").append(escapeAelLiteral(earliestEventId))
                .append("':'").append(escapeAelLiteral(latestEventId)).append("'}");
        if (deviceFilter != null && !deviceFilter.isEmpty()) {
            ael.append("&[?(@.[0] in [");
            boolean first = true;
            for (String deviceId : deviceFilter) {
                if (!first) {
                    ael.append(',');
                }
                first = false;
                ael.append('\'').append(escapeAelLiteral(deviceId)).append('\'');
            }
            ael.append("])]");
        }
        return session.query(key).bin(BIN_NAME).selectFrom(ael.toString()).execute().getFirstRecord();
    }

    private static String escapeAelLiteral(String s) {
        return s.replace("'", "\\'");
    }

    @SuppressWarnings("unchecked")
    private void addEventsToResults(int count, Record record, List<Event> results, SortDirection direction) {
        if (record == null) {
            return;
        }
        List<List<?>> entries = (List<List<?>>) (List<?>) record.getList(BIN_NAME);

        if (direction == SortDirection.DESCENDING) {
            for (int i = entries.size() - 1; i >= 0; i--) {
                if (!addEventToResults(count, entries.get(i), results)) {
                    break;
                }
            }
        }
        else {
            for (List<?> entry : entries) {
                if (!addEventToResults(count, entry, results)) {
                    break;
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private boolean addEventToResults(int count, List<?> value, List<Event> results) {
        if (results.size() >= count) {
            return false;
        }
        Map<String, Object> eventMap = (Map<String, Object>) value.get(1);
        results.add(convertMapToEvent(eventMap));
        return true;
    }

    // ------------------------------------------------------------------
    // Validation
    // ------------------------------------------------------------------

    private void validateEvent(Event event) {
        if (event == null) {
            throw new IllegalArgumentException("Event cannot be null");
        }
        if (event.getAccountId() == null || event.getAccountId().trim().isEmpty()) {
            throw new IllegalArgumentException("Event account ID cannot be null or empty");
        }
        if (event.getTimestamp() == null) {
            throw new IllegalArgumentException("Event timestamp cannot be null");
        }
    }

    private void validateAccountId(String accountId) {
        if (accountId == null || accountId.trim().isEmpty()) {
            throw new IllegalArgumentException("Account ID cannot be null or empty");
        }
    }

    private void validateCount(int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("Count must be positive");
        }
    }

    private void validateTimestamps(Long startTimestamp, Long endTimestamp) {
        if (startTimestamp != null && endTimestamp != null && startTimestamp >= endTimestamp) {
            throw new IllegalArgumentException("startTimestamp must be less than end timestamp");
        }
    }

    // ------------------------------------------------------------------
    // Data generation
    // ------------------------------------------------------------------

    public Event generateSampleEvent(String accountId, String deviceId) {
        long fourteenDaysMs = TimeUnit.DAYS.toMillis(MAX_DAYS_TO_STORE);
        long timestamp = System.currentTimeMillis() - ThreadLocalRandom.current().nextLong(0, fourteenDaysMs);
        return buildEvent(accountId, deviceId, timestamp);
    }

    private Event buildEvent(String accountId, String deviceId, long timestamp) {
        int randomValue = Math.abs(ThreadLocalRandom.current().nextInt());
        Event event = new Event();
        event.setId(String.format("%013d%012d", timestamp, randomValue));
        event.setAccountId(accountId);
        event.setDeviceId(deviceId);
        event.setTimestamp(new Date(timestamp));
        event.setResolution(List.of(1920, 1080));
        event.setParameterTags(List.of("tag-" + ThreadLocalRandom.current().nextInt(5)));
        event.setPartnerId(java.util.UUID.randomUUID().toString());
        event.setPartnerStateId("state " + ThreadLocalRandom.current().nextInt(1000));
        event.setVideoMeta(Map.of("duration", 13, "videoUrl", DEFAULT_VIDEO_URL));
        event.setParameters(Map.of(
                "imageMeta", Map.of("assetId", "", "frameIndex", 0, "storageLocation", DEFAULT_STORAGE_LOCATION),
                "imageUrl", "",
                "objectsDetected", List.of(
                        Map.of("frameIndex", 0, "type", "person"),
                        Map.of("frameIndex", 0, "type", "motion"))));
        return event;
    }

    public static void displayEvents(List<Event> events) {
        for (int i = 0; i < events.size(); i++) {
            Event event = events.get(i);
            System.out.printf("%2d: %s - %s - %s%n", (i + 1), event.getId(), event.getTimestamp(), event.getDeviceId());
        }
    }

    private static <T> T getLastElement(List<T> list) {
        return list.get(list.size() - 1);
    }

    private void generateSampleData(Session session) {
        long accountsCreated = 0;
        long devicesCreated = 0;
        long eventsCreated = 0;

        for (int accountNum = 1; accountNum <= NUM_ACCOUNTS; accountNum++) {
            Account account = new Account("acct-" + accountNum, ThreadLocalRandom.current().nextInt(1, 21));

            if ("acct-1".equals(account.getId())) {
                account.setNumDevices(Math.max(10, account.getNumDevices()));
                long timestamp = new Date().getTime() - TimeUnit.DAYS.toMillis(MAX_DAYS_TO_STORE);
                for (int i = 0; i < NUM_EVENTS_ACCT_1; i++) {
                    timestamp += ThreadLocalRandom.current().nextLong(100);
                    int deviceId = ThreadLocalRandom.current().nextInt(account.getNumDevices());
                    Event event = buildEvent(account.getId(), "device-" + account.getId() + "-" + deviceId, timestamp);
                    upsertEvent(session, event, true);
                    eventsCreated++;
                }
            }
            else {
                for (int deviceNum = 0; deviceNum < account.getNumDevices(); deviceNum++) {
                    int eventsThisDevice = ThreadLocalRandom.current().nextInt(MAX_EVENTS_PER_DEVICE);
                    for (int eventCount = 0; eventCount < eventsThisDevice; eventCount++) {
                        Event event = generateSampleEvent(account.getId(), "device-" + account.getId() + "-" + deviceNum);
                        upsertEvent(session, event, true);
                        eventsCreated++;
                    }
                    devicesCreated++;
                }
            }
            accountsCreated++;
            if (accountsCreated % 10 == 0 || accountsCreated == NUM_ACCOUNTS) {
                System.out.printf("%,d accounts, %,d devices, %,d events%n", accountsCreated, devicesCreated, eventsCreated);
            }
        }
    }

    private void demonstrateQueries(Session session) {
        System.out.println("First list -- acct-1, all devices");
        List<Event> events = getEventsBefore(session, "acct-1", null, 50);
        displayEvents(events);

        events = getEventsBefore(session, "acct-1", getLastElement(events).getId(), 50);
        System.out.println("\nSecond page:");
        displayEvents(events);

        System.out.println("First list -- acct-1, devices 1, 2, 3");
        int pageSize = 25;
        events = getEventsBefore(session, "acct-1", null, pageSize,
                "device-acct-1-1", "device-acct-1-2", "device-acct-1-3");
        displayEvents(events);

        int pageCounter = 1;
        String eventIdAtTopOfPage = null;
        while (events.size() == pageSize && pageCounter < 5) {
            System.out.printf("Page %,d%n", ++pageCounter);
            eventIdAtTopOfPage = getLastElement(events).getId();
            events = getEventsBefore(session, "acct-1", eventIdAtTopOfPage, pageSize,
                    "device-acct-1-1", "device-acct-1-2", "device-acct-1-3");
            displayEvents(events);
        }

        System.out.println("\nGetting NEXT (ascending) 35 events after " + eventIdAtTopOfPage);
        events = getEventsAfter(session, "acct-1", eventIdAtTopOfPage, 35,
                "device-acct-1-1", "device-acct-1-2", "device-acct-1-3");
        displayEvents(events);

        long endTime = new Date().getTime();
        long startTime = endTime - TimeUnit.DAYS.toMillis(2);
        System.out.println("\nShowing last 2 days of events in descending order");
        events = getEventsBetween(session, "acct-1", startTime, endTime, null, 100_000,
                SortDirection.DESCENDING, "device-acct-1-8", "device-acct-1-9", "device-acct-10");
        displayEvents(events);

        System.out.println("Showing events for acct-2, all devices");
        events = getEventsBefore(session, "acct-2", null, 20_000);
        displayEvents(events);
    }
}
