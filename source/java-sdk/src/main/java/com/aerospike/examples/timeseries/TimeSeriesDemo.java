package com.aerospike.examples.timeseries;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.timeseries.model.Account;
import com.aerospike.examples.timeseries.model.Event;

/**
 * SDK port of the legacy {@code TimeSeriesDemo} (see ../../java). Stores IoT device events
 * partitioned by account and bucketed into fixed-width time windows (one record per account per
 * bucket); each record holds a key-ordered map from a time-sortable {@code eventId} to a
 * {@code [deviceId, eventDetails]} pair, so range/pagination queries against a bucket become map
 * key-range operations. Device filtering is also pushed down server-side via an AEL selector-plus-
 * filter expression (see {@link #readFilteredBucket}).
 * <p/>
 * Unlike the legacy version, this port doesn't hold its own {@code IAerospikeClient}/{@code
 * AutoCloseable} lifecycle or a {@code main()} - the runner owns the {@code Session} and passes it
 * to every use case, so all methods here take a {@code Session} parameter instead.
 */
public class TimeSeriesDemo implements UseCase {

    public enum SortDirection {
        ASCENDING,
        DESCENDING
    }

    private static final String BIN_NAME = "map";
    private static final MapOrder MAP_ORDER = MapOrder.KEY_ORDERED;

    private static final long MILLIS_PER_HOUR = TimeUnit.HOURS.toMillis(1);
    private static final long DATE_OFFSET_MILLIS = TimeUnit.DAYS.toMillis(LocalDate.of(2024, 1, 1).toEpochDay());
    private static final int BUCKET_WIDTH_HOURS = 24;
    private static final int MAX_DAYS_TO_STORE = 14;
    private static final int HOURS_PER_DAY = 24;
    private static final int EVENT_ID_TIMESTAMP_LENGTH = 13;
    private static final int EVENT_ID_RANDOM_LENGTH = 12;

    private static final int NUM_ACCOUNTS = 10;
    private static final int MAX_EVENTS_PER_DEVICE = 800;
    private static final String DEFAULT_VIDEO_URL = "https://somewhere.com/4659278373492";
    private static final String DEFAULT_STORAGE_LOCATION = "hv";

    @Override
    public String getName() {
        return "Predictable time-series data";
    }

    @Override
    public String getDescription() {
        return "Demonstrates how to store, update and query time-series data. In this case the data is "
                + "devices which generate events. These devices could be motion sensors, cameras, etc. "
                + "The data model has many accounts, each account has a handful of devices, and the devices "
                + "generate events when triggered. The events are stored for 14 days, and queries can be "
                + "performed on the events for an account, filtering by time range and / or a list of device ids. "
                + "This shows a way to store time series data with events occurring on a sporadic (random) basis, with "
                + "low variation in cardinality, or events occurring on a periodic basis like stock ticks.";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/timeseries.md";
    }

    private final DataSet events = DataSet.of(System.getProperty("demo.namespace", "test"), "uccb_events");

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
    // Bucket / event-id helpers
    // ------------------------------------------------------------------

    public static long getBucketOffset(long timestamp) {
        return (timestamp - DATE_OFFSET_MILLIS) / (MILLIS_PER_HOUR * BUCKET_WIDTH_HOURS);
    }

    private Key createEventKey(String accountId, long timestamp) {
        return events.id(accountId + ":" + getBucketOffset(timestamp));
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

    /**
     * Forms an eventId from a timestamp. If {@code lowerBound} is true, the unique-id portion is
     * all zeros; otherwise it's all nines - i.e. the lowest/highest possible eventId for that
     * timestamp.
     */
    private static String eventIdFromTimestamp(long timestamp, boolean lowerBound) {
        if (lowerBound) {
            return String.format("%013d%012d", timestamp, 0);
        }
        else {
            return String.format("%013d%d", timestamp, 999_999_999_999L);
        }
    }

    private static String generateNextEventId(String eventId) {
        long value = Long.parseLong(eventId.substring(EVENT_ID_TIMESTAMP_LENGTH));
        return String.format("%s%012d", eventId.substring(0, EVENT_ID_TIMESTAMP_LENGTH), value + 1);
    }

    // ------------------------------------------------------------------
    // Event <-> Map conversion (events live nested inside a bucket record's map bin, not as
    // their own top-level record, so they're not a fit for RecordMapper/object mapping)
    // ------------------------------------------------------------------

    private static Map<String, Object> convertEventToMap(Event event) {
        Map<String, Object> map = new java.util.HashMap<>();
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
    // Database operations
    // ------------------------------------------------------------------

    /**
     * Inserts or updates an event. If {@code setExpiry} is true, the bucket record's TTL is set
     * to the retention window; otherwise its TTL is left unchanged.
     */
    private static volatile boolean expirationWarningShown = false;

    public void upsertEvent(Session session, Event event, boolean setExpiry) {
        validateEvent(event);
        long eventTimeMillis = event.getTimestamp().getTime();
        Key key = createEventKey(event.getAccountId(), eventTimeMillis);

        try {
            writeEvent(session, key, event, setExpiry);
        }
        catch (com.aerospike.client.sdk.AerospikeException ae) {
            // Some namespaces (eviction/nsup disabled, e.g. this dev cluster's "test" namespace)
            // reject an explicit record TTL with FAIL_FORBIDDEN. Fall back to writing without one.
            if (setExpiry && ae.getResultCode() == com.aerospike.client.sdk.ResultCode.FAIL_FORBIDDEN) {
                if (!expirationWarningShown) {
                    expirationWarningShown = true;
                    System.out.println(com.aerospike.examples.AnsiColors.YELLOW
                            + "Note: this namespace does not support record expiration (eviction is disabled) - "
                            + "events will be written without a TTL." + com.aerospike.examples.AnsiColors.RESET);
                }
                writeEvent(session, key, event, false);
            }
            else {
                throw ae;
            }
        }
    }

    private void writeEvent(Session session, Key key, Event event, boolean setExpiry) {
        var op = session.upsert(key);
        op = setExpiry ? op.expireRecordAfter(java.time.Duration.ofDays(MAX_DAYS_TO_STORE)) : op.withNoChangeInExpiration();
        op.bin(BIN_NAME).onMapKey(event.getId(), MAP_ORDER)
                .upsert(List.of(event.getDeviceId(), convertEventToMap(event)))
                .execute();
    }

    private long getOldestTimestamp(Long startTimestamp) {
        return startTimestamp == null ? new Date().getTime() - TimeUnit.DAYS.toMillis(MAX_DAYS_TO_STORE) : startTimestamp;
    }

    private long getLatestTimestamp(Long endTimestamp) {
        return endTimestamp == null ? new Date().getTime() : endTimestamp;
    }

    /**
     * Retrieves events for an account between the given date range, starting with the newest (or
     * oldest, per {@code direction}). If {@code eventId} is passed, results are exclusive of it,
     * allowing this to be used for pagination.
     */
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
                Record record = readFilteredBucket(session, events.id(accountId + ":" + recordKey), earliestEventId, latestEventId, deviceFilter);
                addEventsToResults(count, record, results, direction);
            }
        }
        else {
            for (long recordKey = endRecord; results.size() < count && recordKey >= startRecord; recordKey--) {
                Record record = readFilteredBucket(session, events.id(accountId + ":" + recordKey), earliestEventId, latestEventId, deviceFilter);
                addEventsToResults(count, record, results, direction);
            }
        }

        return results;
    }

    public List<Event> getEventsBefore(Session session, String accountId, String eventId, int count, String... deviceIds) {
        return getEventsBetween(session, accountId, null, null, eventId, count, SortDirection.DESCENDING, deviceIds);
    }

    public List<Event> getEventsAfter(Session session, String accountId, String eventId, int count, String... deviceIds) {
        return getEventsBetween(session, accountId, null, null, eventId, count, SortDirection.ASCENDING, deviceIds);
    }

    public long getTotalEventsForAccount(Session session, String accountId) {
        validateAccountId(accountId);

        long now = new Date().getTime();
        long firstRecord = getBucketOffset(now);
        int bucketsForTimeRange = (MAX_DAYS_TO_STORE * HOURS_PER_DAY + BUCKET_WIDTH_HOURS - 1) / BUCKET_WIDTH_HOURS;
        long endRecord = firstRecord - bucketsForTimeRange;

        List<Key> keys = new ArrayList<>();
        for (long i = endRecord; i <= firstRecord; i++) {
            keys.add(events.id(accountId + ":" + i));
        }

        AtomicLong totalEvents = new AtomicLong();
        try (var stream = session.query(keys).bin(BIN_NAME).mapSize().execute()) {
            stream.forEach(result -> {
                if (result.isOk()) {
                    totalEvents.addAndGet(result.recordOrThrow().getLong(BIN_NAME));
                }
            });
        }
        return totalEvents.get();
    }

    // ------------------------------------------------------------------
    // Filtering helpers
    // ------------------------------------------------------------------

    /**
     * Reads a bucket's events restricted to an eventId key range, with device filtering (when
     * requested) also pushed down server-side.
     * <p/>
     * The legacy version's device filter is a nested map expression ({@code
     * MapExp.getByValueList} over the result of {@code MapExp.getByKeyRange}, matching entries
     * against a wildcard-tailed {@code [deviceId, *]} value list); an earlier pass at this port
     * tried the equivalent in raw {@code Exp}/{@code MapExp} builder calls and concluded it wasn't
     * expressible against this alpha SDK build. Re-checked against the canonical AEL reference
     * (courtesy of Tim Faulkes - see ../../AEL_CANONICAL_REFERENCE.md §4.4, §5, §7) instead of
     * that builder API, and it works directly: a map key-range selector chained with a filter
     * (not a wildcard - {@code &[?(…)]} is the selector-then-filter form in §4.4, not {@code
     * .*[?(…)]}) against the loop variable's first list element:
     * {@code $.map.{@'<earliest>':'<latest>'}&[?(@.[0] in ['dev1','dev2'])]}. Verified live: the
     * filtered read returns exactly the matching {@code [deviceId, eventDetails]} entries, still
     * in key order (implicit-get on a map-range/filter path returns a flat LIST of values, so no
     * separate {@code getKeysAndValues()}-style terminal is needed - see {@link
     * #addEventsToResults}), with zero matches correctly returning an empty list rather than an
     * error.
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
        // The AEL selector's implicit get returns a flat LIST of [deviceId, eventDetails] pairs
        // (already key-range- and device-filtered server-side), in key order.
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
        event.setVideoMeta(Map.of(
                "duration", 13,
                "videoUrl", DEFAULT_VIDEO_URL));
        event.setParameters(Map.of(
                "imageMeta", Map.of(
                        "assetId", "",
                        "frameIndex", 0,
                        "storageLocation", DEFAULT_STORAGE_LOCATION),
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
        AtomicLong accountsCreated = new AtomicLong();
        AtomicLong devicesCreated = new AtomicLong();
        AtomicLong eventsCreated = new AtomicLong();

        for (int accountNum = 1; accountNum <= NUM_ACCOUNTS; accountNum++) {
            Account account = new Account("acct-" + accountNum, ThreadLocalRandom.current().nextInt(1, 21));
            if ("acct-1".equals(account.getId())) {
                account.setNumDevices(Math.max(10, account.getNumDevices()));
            }

            for (int deviceNum = 0; deviceNum < account.getNumDevices(); deviceNum++) {
                int eventsThisDevice = ThreadLocalRandom.current().nextInt(MAX_EVENTS_PER_DEVICE);
                for (int eventCount = 0; eventCount < eventsThisDevice; eventCount++) {
                    Event event = generateSampleEvent(account.getId(), "device-" + account.getId() + "-" + deviceNum);
                    upsertEvent(session, event, true);
                    eventsCreated.incrementAndGet();
                }
                devicesCreated.incrementAndGet();
            }
            accountsCreated.incrementAndGet();
            System.out.printf("%,d accounts, %,d devices, %,d events%n",
                    accountsCreated.get(), devicesCreated.get(), eventsCreated.get());
        }
    }

    private void demonstrateQueries(Session session) {
        System.out.printf("Account acct-1 has %,d events%n%n", getTotalEventsForAccount(session, "acct-1"));

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
        while (events.size() == pageSize) {
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

        System.out.println("\nShowing last 2 days of events in ascending order");
        long now = System.nanoTime();
        events = getEventsBetween(session, "acct-1", startTime, endTime, null, 100_000,
                SortDirection.ASCENDING, "device-acct-1-8", "device-acct-1-9", "device-acct-10");
        long totalTime = System.nanoTime() - now;
        System.out.printf("Time taken: %,dus%n", totalTime / 1000);
        displayEvents(events);
    }
}
