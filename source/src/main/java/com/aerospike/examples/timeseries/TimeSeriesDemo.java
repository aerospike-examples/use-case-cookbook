package com.aerospike.examples.timeseries;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.timeseries.model.Account;
import com.aerospike.examples.timeseries.model.Event;
import com.aerospike.generator.Generator;
import com.aerospike.generator.ValueCreator;
import com.aerospike.mapper.tools.AeroMapper;

/**
 * Event data management system 
 * 
 * This class provides functionality to store, retrieve, and manage event data
 * for IoT devices with high-performance requirements. The system is designed to
 * handle large-scale event data with sub-500ms P99 performance for paginated
 * queries and sub-100ms for direct lookups.
 * 
 * <h3>Data Model</h3>
 * Events are stored with the following structure:
 * <ul>
 *   <li>Account-based partitioning for scalability</li>
 *   <li>Time-based day offsets for efficient range queries</li>
 *   <li>Device-specific filtering capabilities</li>
 *   <li>14-day retention policy for image data</li>
 * </ul>
 * 
 */
public class TimeSeriesDemo implements UseCase, AutoCloseable {
    
    // ============================================================================
    // CONSTANTS AND CONFIGURATION
    // ============================================================================
    
    /** Sort direction for event queries */
    public enum SortDirection {
        ASCENDING,
        DESCENDING
    }
    
    /** Database configuration constants */
    public static final class DatabaseConfig {
        public static String NAMESPACE = "test";
        public static String EVENT_SET = "events";
        public static final String BIN_NAME = "map";
        public static final String HOST = "localhost";
        public static final int PORT = 3100;
        
        private DatabaseConfig() {} // Prevent instantiation
    }
    
    /** Time and date configuration */
    public static final class TimeConfig {
        public static final long MILLIS_PER_HOUR = TimeUnit.HOURS.toMillis(1);
        public static final long DATE_OFFSET_MILLIS = TimeUnit.DAYS.toMillis(LocalDate.of(2024, 1, 1).toEpochDay());
        public static final int BUCKET_WIDTH_HOURS = 24;
        public static final int MAX_DAYS_TO_STORE = 14;
        public static final int HOURS_PER_DAY = 24;
        public static final int EVENT_ID_TIMESTAMP_LENGTH = 13;
        public static final int EVENT_ID_RANDOM_LENGTH = 12;
        
        private TimeConfig() {} // Prevent instantiation
    }
    
    /** Data generation configuration */
    public static final class GenerationConfig {
        public static final int NUM_ACCOUNTS = 10;
        public static final int MAX_DEVICES_PER_ACCOUNT = 8;
        public static final int MAX_EVENTS_PER_DEVICE = 800;
        public static final String DEFAULT_VIDEO_URL = "https://somewhere.com/4659278373492";
        public static final String DEFAULT_STORAGE_LOCATION = "hv";
        
        private GenerationConfig() {} // Prevent instantiation
    }
    
    // ============================================================================
    // INSTANCE VARIABLES
    // ============================================================================
    
    private IAerospikeClient client;
    private final MapPolicy mapPolicy;
    private final ValueCreator<Event> eventCreator;
    
    // ============================================================================
    // CONSTRUCTORS
    // ============================================================================
    
    /**
     * Creates a new Runner instance with the specified Aerospike client.
     */
    public TimeSeriesDemo() {
        this.mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        this.eventCreator = new ValueCreator<>(Event.class);
    }
    
    /**
     * Creates a new Runner instance with a default Aerospike client configuration.
     * 
     * @return A configured Runner instance
     */
    public static TimeSeriesDemo createWithDefaultClient() {
        ClientPolicy clientPolicy = createDefaultClientPolicy();
        IAerospikeClient client = new AerospikeClient(clientPolicy, DatabaseConfig.HOST, DatabaseConfig.PORT);
        AeroMapper mapper = new AeroMapper.Builder(client).build();
        return new TimeSeriesDemo().setClient(client, mapper);
    }

    public TimeSeriesDemo setClient(IAerospikeClient client, AeroMapper mapper) {
        this.client = client;
        DatabaseConfig.NAMESPACE = mapper.getNamespace(Event.class);
        DatabaseConfig.EVENT_SET = mapper.getSet(Event.class);
        return this;
    }
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
                + "This show a way to store time series data with events occurring on a sporadic (random) basis, with "
                + "low variation in cardinality, or events occuring on a periodic basis like stock ticks.";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/timeseries.md";
    }
    
    @Override
    public String[] getTags() {
        return new String[] {"Map operations", "Nested CDT expressions", "Timeseries"};
    }
    
    @Override
    public void setup(IAerospikeClient client, AeroMapper mapper) throws Exception {
        setClient(client, mapper);
        
        // Clear existing data
        client.truncate(null, DatabaseConfig.NAMESPACE, DatabaseConfig.EVENT_SET, null);
        generateSampleData();
    }

    @Override
    public void run(IAerospikeClient client, AeroMapper mapper) throws Exception {
        demonstrateQueries();
    }

    // ============================================================================
    // UTILITY METHODS
    // ============================================================================
    
    /**
     * Calculates the bucket offset from the reference date for a given timestamp.
     * 
     * @param timestamp - The timestamp in milliseconds
     * @return The bucket offset from the reference date
     */
    public static long getBucketOffset(long timestamp) {
        return (timestamp - TimeConfig.DATE_OFFSET_MILLIS) / (TimeConfig.MILLIS_PER_HOUR * TimeConfig.BUCKET_WIDTH_HOURS);
    }
    
    /**
     * Creates a database key for an event based on account ID and timestamp.
     * 
     * @param accountId - The account identifier
     * @param timestamp - The event timestamp in milliseconds
     * @return The database key for the event
     */
    public static Key createEventKey(String accountId, long timestamp) {
        return new Key(DatabaseConfig.NAMESPACE, DatabaseConfig.EVENT_SET, 
                      accountId + ":" + getBucketOffset(timestamp));
    }
    
    /**
     * Converts a Date object to a long timestamp.
     * 
     * @param date - The date to convert, may be null
     * @return The timestamp in milliseconds, or 0 if date is null
     */
    private static long dateToLong(Date date) {
        return date == null ? 0 : date.getTime();
    }
    
    /**
     * Converts a long timestamp to a Date object.
     * 
     * @param timestamp - The timestamp in milliseconds
     * @return The Date object, or null if timestamp is 0
     */
    private static Date longToDate(long timestamp) {
        return timestamp == 0 ? null : new Date(timestamp);
    }
    
    /**
     * Extracts the timestamp portion from an event ID.
     * 
     * @param eventId - The event ID string
     * @return The timestamp in milliseconds
     * @throws NumberFormatException if the event ID format is invalid
     */
    private static long extractTimestampFromEventId(String eventId) {
        return Long.parseLong(eventId.substring(0, TimeConfig.EVENT_ID_TIMESTAMP_LENGTH));
    }
    
    /**
     * Form an eventId from a timestamp. If {@code lowerBound} is {@code true} then 
     * the unique id portion is set to all zeros, otherwise it's set to all nines. 
     * @param timestamp - the timestamp part of the eventId
     * @param lowerBound - whether to make this the lowest value or highest value eventId for the passed timestamp
     * @return the eventId starting with the passed timestamp
     */
    private static String eventIdFromTimestamp(long timestamp, boolean lowerBound) {
        if (lowerBound) {
            return String.format("%013d%012d", timestamp, 0);
        }
        else {
            return String.format("%013d%d", timestamp, 999_999_999_999l);
        }
    }
    
    /**
     * Generates the next event ID after the given event ID for pagination.
     * 
     * @param eventId - The current event ID
     * @return The next event ID in sequence
     */
    private static String generateNextEventId(String eventId) {
        long value = Long.parseLong(eventId.substring(TimeConfig.EVENT_ID_TIMESTAMP_LENGTH));
        return String.format("%s%012d", 
                           eventId.substring(0, TimeConfig.EVENT_ID_TIMESTAMP_LENGTH), 
                           value + 1);
    }

    /**
     * Generates the event ID before the given event ID for pagination.
     * 
     * @param eventId - The current event ID
     * @return The prior event ID in sequence
     */
    private static String generatePriorEventId(String eventId) {
        long randomPart = Long.parseLong(eventId.substring(eventId.length() - TimeConfig.EVENT_ID_RANDOM_LENGTH));
        if (randomPart > 0) {
            return String.format("%s%012d", 
                    eventId.substring(0, eventId.length() - TimeConfig.EVENT_ID_RANDOM_LENGTH), 
                    randomPart - 1);
        }
        else {
            long timestampPart = Long.parseLong(eventId.substring(0, TimeConfig.EVENT_ID_TIMESTAMP_LENGTH));
            return eventIdFromTimestamp(timestampPart-1, false);
        }
        
    }
    
    // ============================================================================
    // DATA TRANSFORMATION METHODS
    // ============================================================================
    
    /**
     * Converts an Event object to a Map representation for storage.
     * 
     * @param event - The event to convert
     * @return A Map representation of the event
     * @throws IllegalArgumentException if event is null
     */
    public static Map<String, Object> convertEventToMap(Event event) {
        if (event == null) {
            throw new IllegalArgumentException("Event cannot be null");
        }
        
        return Map.of(
            "id", event.getId(),
            "accountId", event.getAccountId(),
            "deviceId", event.getDeviceId(),
            "params", event.getParameters(),
            "resolution", event.getResolution(),
            "videoMeta", event.getVideoMeta(),
            "paramTags", event.getParameterTags(),
            "partnerId", event.getPartnerId(),
            "partStateId", event.getPartnerStateId(),
            "timestamp", dateToLong(event.getTimestamp())
        );
    }
    
    /**
     * Converts a Map representation back to an Event object.
     * 
     * @param eventMap The map representation of the event
     * @return The Event object, or null if eventMap is null
     * @throws ClassCastException if the map contains invalid data types
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Event convertMapToEvent(Map<String, Object> eventMap) {
        if (eventMap == null) {
            return null;
        }
        
        Event event = new Event();
        event.setId((String) eventMap.get("id"));
        event.setAccountId((String) eventMap.get("accountId"));
        event.setDeviceId((String) eventMap.get("deviceId"));
        event.setParameters((Map) eventMap.get("params"));
        event.setResolution((List) eventMap.get("resolution"));
        event.setVideoMeta((Map) eventMap.get("videoMeta"));
        event.setParameterTags((List) eventMap.get("paramTags"));
        event.setPartnerId((String) eventMap.get("partnerId"));
        event.setPartnerStateId((String) eventMap.get("partStateId"));
        event.setTimestamp(longToDate((Long) eventMap.get("timestamp")));
        
        return event;
    }
    
    // ============================================================================
    // DATABASE OPERATIONS
    // ============================================================================
    
    /**
     * Inserts or updates an event in the database.
     * 
     * @param event - The event to upsert
     * @param setExpiry - If this parameter is true, set the TTL of the record. If
     * it's false, set the exipry to be -2 (do not change TTL)
     *
     * @throws IllegalArgumentException if event is null or missing required fields
     */
    public void upsertEvent(Event event, boolean setExpiry) {
        validateEvent(event);
        
        long eventTimeMillis = event.getTimestamp().getTime();
        Key key = createEventKey(event.getAccountId(), eventTimeMillis);
        
        WritePolicy writePolicy = client.copyWritePolicyDefault();
        if (setExpiry) {
            writePolicy.expiration = (int)TimeUnit.DAYS.toSeconds(TimeConfig.MAX_DAYS_TO_STORE);
        }
        else {
            writePolicy.expiration = -2; // Do not alter TTL
        }
        client.operate(writePolicy, key, 
            MapOperation.put(mapPolicy, DatabaseConfig.BIN_NAME, 
                Value.get(event.getId()), 
                Value.get(List.of(event.getDeviceId(), convertEventToMap(event)))
            )
        );
    }
    
    /**
     * get the starting (oldest) timestamp
     */
    private long getOldestTimestamp(Long startTimestamp) {
        if (startTimestamp == null) {
            return new Date().getTime() - TimeUnit.DAYS.toMillis(TimeConfig.MAX_DAYS_TO_STORE);
        }
        else {
            return startTimestamp;
        }
    }
    
    /**
     * Get the ending (latest) timestamp
     */
    private long getLatestTimestamp(Long endTimestamp) {
        if (endTimestamp == null) {
            return new Date().getTime();
        }
        else {
            return endTimestamp;
        }
    }
    
    /**
     * Retrieves events for an account between the specified date range, starting with the 
     * newest ones and descending in time. If an {@code eventId} is passed, no events after or including
     * that one will be returned, allowing this to be used for pagination.
     * 
     * @param accountId - The account identifier
     * @param startTimestamp - The starting timestamp (inclusive)
     * @param endTimestamp - The ending timestamp (inclusive) 
     * @param eventId - The event ID to start from (null for most recent)
     * @param count - Maximum number of events to retrieve
     * @param deviceIds - Optional device IDs to filter by
     * @return List of events sorted by newest first
     * @throws IllegalArgumentException if accountId is null or count is invalid or the time range is invalie
     */
    public List<Event> getEventsBetween(String accountId, Long startTimestamp, Long endTimestamp,
            String eventId, int count, SortDirection direction, String... deviceIds) {
        
        validateAccountId(accountId);
        validateCount(count);
        validateTimestamps(startTimestamp, endTimestamp);
        
        List<Event> results = new ArrayList<>();

        String latestEventId;
        String earliestEventId;
        
        if (eventId != null) {
            if (direction == SortDirection.ASCENDING) {
                // Run from AFTER event id up to endTimestamp, or now if not specified
                earliestEventId = generateNextEventId(eventId);
                latestEventId = eventIdFromTimestamp(getLatestTimestamp(endTimestamp), false);
            }
            else  {
                // Run from startTimestamp, or now - 14 days, to the eventId. Note that as the
                // end event is exclusive, we do not need to calculate a prior event id.
                latestEventId = eventId;
                earliestEventId = eventIdFromTimestamp(getOldestTimestamp(startTimestamp), true);
            }
        }
        else {
            // Full date range, don't care about ascending or descending
            latestEventId = eventIdFromTimestamp(getLatestTimestamp(endTimestamp), false);
            earliestEventId = eventIdFromTimestamp(getOldestTimestamp(startTimestamp), true);
        }
        
        long startRecord = getBucketOffset(extractTimestampFromEventId(earliestEventId));
        long endRecord = getBucketOffset(extractTimestampFromEventId(latestEventId));
        
        Operation operation = createFilterOperation(earliestEventId, latestEventId, count, deviceIds);

        if (direction == SortDirection.ASCENDING) {
            for (long recordKey = startRecord; 
                    results.size() < count && recordKey <= endRecord; 
                    recordKey++) {
                   
               Key key = new Key(DatabaseConfig.NAMESPACE, DatabaseConfig.EVENT_SET, accountId + ":" + recordKey);
               Record record = client.operate(null, key, operation);
               addEventsToResults(count, record, results, direction);
            }            
        }
        else {
            for (long recordKey = endRecord; 
                    results.size() < count && recordKey >= startRecord; 
                    recordKey--) {
                   
               Key key = new Key(DatabaseConfig.NAMESPACE, DatabaseConfig.EVENT_SET, accountId + ":" + recordKey);
               Record record = client.operate(null, key, operation);
               addEventsToResults(count, record, results, direction);
            }            
        }
        
        return results;
    }

    
    /**
     * Retrieves events for an account before a specified event ID.
     * 
     * @param accountId - The account identifier
     * @param eventId - The event ID to start from (null for most recent)
     * @param count - Maximum number of events to retrieve
     * @param deviceIds - Optional device IDs to filter by
     * @return List of events sorted by newest first
     * @throws IllegalArgumentException if accountId is null or count is invalid
     */
    public List<Event> getEventsBefore(String accountId, String eventId, int count, String... deviceIds) {
        return getEventsBetween(accountId, null, null, eventId, count, SortDirection.DESCENDING, deviceIds);
    }
    
    /**
     * Retrieves events for an account after a specified event ID.
     * 
     * @param accountId - The account identifier
     * @param eventId - The event ID to start from (required)
     * @param count - Maximum number of events to retrieve
     * @param deviceIds - Optional device IDs to filter by
     * @return List of events sorted by oldest first
     * @throws IllegalArgumentException if eventId is null, accountId is null, or count is invalid
     */
    public List<Event> getEventsAfter(String accountId, String eventId, int count, String... deviceIds) {
        return getEventsBetween(accountId, null, null, eventId, count, SortDirection.ASCENDING, deviceIds);
    }
    
    /**
     * Calculates the total number of events for an account.
     * 
     * @param accountId - The account identifier
     * @return The total number of events
     * @throws IllegalArgumentException if accountId is null
     */
    public long getTotalEventsForAccount(String accountId) {
        validateAccountId(accountId);
        
        long now = new Date().getTime();
        long firstRecord = getBucketOffset(now);
        int bucketsForTimeRange = (int)((TimeConfig.MAX_DAYS_TO_STORE * TimeConfig.HOURS_PER_DAY + TimeConfig.BUCKET_WIDTH_HOURS - 1)
                / TimeConfig.BUCKET_WIDTH_HOURS);
        long endRecord = firstRecord - bucketsForTimeRange;
        Key[] keys = new Key[bucketsForTimeRange + 1];
        
        for (int i = (int)endRecord; i <= firstRecord; i++) {
            keys[i-(int)endRecord] = new Key(DatabaseConfig.NAMESPACE, DatabaseConfig.EVENT_SET, 
                             accountId + ":" + i);
        }
        
        BatchPolicy batchPolicy = client.copyBatchPolicyDefault();
        batchPolicy.maxConcurrentThreads = 0;
        Record[] records = client.get(batchPolicy, keys, MapOperation.size(DatabaseConfig.BIN_NAME));
        
        long totalEvents = 0;
        for (Record record : records) {
            totalEvents += (record == null) ? 0 : record.getLong(DatabaseConfig.BIN_NAME);
        }
        
        return totalEvents;
    }
    
    // ============================================================================
    // PRIVATE HELPER METHODS
    // ============================================================================
    
    /**
     * Creates a filter operation for database queries.
     */
    private Operation createFilterOperation(String upperBoundEventId, String lowerBoundEventId,
            int count, String... deviceIds) {
        
        if (deviceIds.length == 0) {
            return createAllDevicesFilter(upperBoundEventId, lowerBoundEventId);
        } else {
            return createSpecificDevicesFilter(upperBoundEventId, lowerBoundEventId, deviceIds);
        }
    }
    
    /**
     * Creates a filter operation for all devices.
     */
    private Operation createAllDevicesFilter(String oldestEventId, String newestEventId) {
        Value oldestValue = oldestEventId == null ? Value.NULL : Value.get(oldestEventId);
        Value newestValue = newestEventId == null ? Value.INFINITY : Value.get(newestEventId); 
        
        return MapOperation.getByKeyRange(DatabaseConfig.BIN_NAME, oldestValue, 
                                       newestValue, MapReturnType.KEY_VALUE);
    }
    
    /**
     * Creates a filter operation for specific devices.
     */
    private Operation createSpecificDevicesFilter(String oldestEventId, String newestEventId, String... deviceIds) {
        List<Value> valueList = Arrays.stream(deviceIds)
            .map(deviceId -> Value.get(List.of(deviceId, Value.WILDCARD)))
            .collect(Collectors.toList());
        
        return createTimeRangeAndDeviceFilter(oldestEventId, newestEventId, valueList);
    }
    
    /**
     * Creates a combined time range and device filter.
     */
    private Operation createTimeRangeAndDeviceFilter(String oldestEventId, String newestEventId, List<Value> valueList) {
        Exp filterMapByKeyRange = MapExp.getByKeyRange(MapReturnType.KEY_VALUE, 
                Exp.val(oldestEventId), Exp.val(newestEventId), Exp.mapBin(DatabaseConfig.BIN_NAME));
        
        Exp filterKeyRangeByDevice = MapExp.getByValueList(MapReturnType.KEY_VALUE, 
                Exp.val(valueList), filterMapByKeyRange);
        
        return ExpOperation.read(DatabaseConfig.BIN_NAME, 
            Exp.build(filterKeyRangeByDevice), ExpReadFlags.DEFAULT);
    }
    
    /**
     * Adds events from a database record to the results list.
     */
    private void addEventsToResults(int count, Record record, List<Event> results, SortDirection direction) {
        if (record == null) {
            return;
        }
        
        @SuppressWarnings("unchecked")
        List<Entry<String, List<?>>> entryList = (List<Entry<String, List<?>>>) record.getList(DatabaseConfig.BIN_NAME);
        
        if (direction == SortDirection.DESCENDING) {
            for (int i = entryList.size() - 1; i >= 0; i--) {
                Entry<String, List<?>> entry = entryList.get(i);
                if (!addEventToResults(count, entry.getValue(), results)) {
                    break;
                }
            }
        } else {
            for (Entry<String, List<?>> entry : entryList) {
                if (!addEventToResults(count, entry.getValue(), results)) {
                    break;
                }
            }
        }
    }
    
    /**
     * Adds a single event to the results list if within the count limit.
     */
    private boolean addEventToResults(int count, List<?> value, List<Event> results) {
        if (results.size() >= count) {
            return false;
        }
        
        @SuppressWarnings("unchecked")
        Map<String, Object> eventMap = (Map<String, Object>) value.get(1);
        results.add(convertMapToEvent(eventMap));
        return true;
    }
    
    // ============================================================================
    // VALIDATION METHODS
    // ============================================================================
    
    /**
     * Validates an event object.
     */
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
    
    /**
     * Validates an account ID.
     */
    private void validateAccountId(String accountId) {
        if (accountId == null || accountId.trim().isEmpty()) {
            throw new IllegalArgumentException("Account ID cannot be null or empty");
        }
    }
    
    /**
     * Validates a count parameter.
     */
    private void validateCount(int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("Count must be positive");
        }
    }
    
    /**
     * Validates that the start timestamp is before the endtimestamp if both exist
     */
    private void validateTimestamps(Long startTimestamp, Long endTimestamp) {
        if (startTimestamp != null && endTimestamp != null && startTimestamp >= endTimestamp) {
            throw new IllegalArgumentException("startTimestamp must be less than end timestamp");
        }
    }
    
    // ============================================================================
    // DATA GENERATION METHODS
    // ============================================================================
    
    /**
     * Generates a sample event for testing purposes.
     * 
     * @param accountId The account identifier
     * @param deviceId The device identifier
     * @return A generated event
     */
    public Event generateSampleEvent(String accountId, String deviceId) {
        Event event = eventCreator.createAndPopulate(new HashMap<>(Map.of("Key", 1)));
        
        // Create a 25-character event ID from timestamp and random number
        long timestamp = event.getTimestamp().getTime();
        int randomValue = Math.abs(ThreadLocalRandom.current().nextInt());
        event.setId(String.format("%013d%012d", timestamp, randomValue));
        
        event.setAccountId(accountId);
        event.setDeviceId(deviceId);
        event.setVideoMeta(Map.of(
            "duration", 13, 
            "videoUrl", GenerationConfig.DEFAULT_VIDEO_URL
        ));
        
        event.setParameters(Map.of(
            "imageMeta", Map.of(
                "assetId", "",
                "frameIndex", 0,
                "storageLocation", GenerationConfig.DEFAULT_STORAGE_LOCATION
            ),
            "imageUrl", "",
            "objectsDetected", List.of(
                Map.of("frameIndex", 0, "type", "person"),
                Map.of("frameIndex", 0, "type", "motion")
            )
        ));
        
        return event;
    }
    
    // ============================================================================
    // CLIENT CONFIGURATION
    // ============================================================================
    
    /**
     * Creates a default client policy configuration.
     */
    private static ClientPolicy createDefaultClientPolicy() {
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;
        
        Policy readPolicy = new Policy();
        readPolicy.sendKey = true;
        
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.writePolicyDefault = writePolicy;
        clientPolicy.readPolicyDefault = readPolicy;
        
        return clientPolicy;
    }
    
    // ============================================================================
    // UTILITY METHODS FOR DEMONSTRATION
    // ============================================================================
    
    /**
     * Displays a list of events in a formatted manner.
     * 
     * @param events The list of events to display
     */
    public static void displayEvents(List<Event> events) {
        for (int i = 0; i < events.size(); i++) {
            Event event = events.get(i);
            System.out.printf("%2d: %s - %s - %s%n", 
                (i + 1), event.getId(), event.getTimestamp(), event.getDeviceId());
        }
    }
    
    /**
     * Gets the last element from a list.
     * 
     * @param list - The list to get the last element from
     * @return The last element
     * @throws IndexOutOfBoundsException if the list is empty
     */
    private static <T> T getLastElement(List<T> list) {
        return list.get(list.size() - 1);
    }
    
     // ============================================================================
     // RESOURCE MANAGEMENT
     // ============================================================================
     
     /**
      * Closes the Aerospike client and releases associated resources.
      */
     @Override
     public void close() {
         if (client != null) {
             client.close();
         }
     }
     
     /**
      * Generates sample data for demonstration purposes.
      */
     private void generateSampleData() throws InterruptedException {
         AtomicLong accountsCreated = new AtomicLong();
         AtomicLong devicesCreated = new AtomicLong();
         AtomicLong eventsCreated = new AtomicLong();

         // These objects can get large, which might cause device overflow errors. Limit the number of 
         // generator threads to 1 to prevent this.
         new Generator().generate(1, GenerationConfig.NUM_ACCOUNTS, 1, Account.class, account -> {
             // Force account 1 to have at least 10 devices for demonstration
             if ("acct-1".equals(account.getId())) {
                 account.setNumDevices(Math.max(10, account.getNumDevices()));
             }
             
             int devicesThisAccount = account.getNumDevices();
             for (int deviceNum = 0; deviceNum < devicesThisAccount; deviceNum++) {
                 int eventsThisDevice = ThreadLocalRandom.current().nextInt(GenerationConfig.MAX_EVENTS_PER_DEVICE);
                 for (int eventCount = 0; eventCount < eventsThisDevice; eventCount++) {
                     Event event = generateSampleEvent(account.getId(), 
                         "device-" + account.getId() + "-" + deviceNum);
                     upsertEvent(event, true);
                     eventsCreated.incrementAndGet();
                 }
                 devicesCreated.incrementAndGet();
             }
             accountsCreated.incrementAndGet();
         })
         .monitor(() -> String.format("%,d accounts, %,d devices, %,d events", 
             accountsCreated.get(), devicesCreated.get(), eventsCreated.get()));
     }
    
    /**
     * Demonstrates various query operations.
     */
    private void demonstrateQueries() {
        System.out.printf("Account acct-1 has %,d events%n%n", 
            getTotalEventsForAccount("acct-1"));
        
        // Demonstrate pagination for all devices
        System.out.println("First list -- acct-1, all devices");
        List<Event> events = getEventsBefore("acct-1", null, 50);
        displayEvents(events);
        
        events = getEventsBefore("acct-1", getLastElement(events).getId(), 50);
        System.out.println("\nSecond page:");
        displayEvents(events);
        
        // Demonstrate device-specific filtering
        System.out.println("First list -- acct-1, devices 1, 2, 3");
        int pageSize = 25;
        events = getEventsBefore("acct-1", null, pageSize, 
            "device-acct-1-1", "device-acct-1-2", "device-acct-1-3");
        displayEvents(events);
        
        int pageCounter = 1;
        String eventIdAtTopOfPage = null;
        while (events.size() == pageSize) {
            System.out.printf("Page %,d%n", ++pageCounter);
            eventIdAtTopOfPage = getLastElement(events).getId();
            events = getEventsBefore("acct-1", eventIdAtTopOfPage, pageSize, 
                "device-acct-1-1", "device-acct-1-2", "device-acct-1-3");
            displayEvents(events);
        }
        
        // Demonstrate ascending query
        System.out.println("\nGetting NEXT (ascending) 35 events after " + eventIdAtTopOfPage);
        events = getEventsAfter("acct-1", eventIdAtTopOfPage, 35, 
            "device-acct-1-1", "device-acct-1-2", "device-acct-1-3");
        displayEvents(events);
        
        // Demonstrate ascending and descending queries by time ranges
        long endTime = new Date().getTime();
        long startTime = endTime - TimeUnit.DAYS.toMillis(2);
        System.out.println("\nShowing last 2 days of events in descenting order");
        events = getEventsBetween("acct-1", startTime, endTime, null, 100_000, 
                SortDirection.DESCENDING, "device-acct-1-8", "device-acct-1-9", "device-acct-10");
        displayEvents(events);

        System.out.println("\nShowing last 2 days of events in ascenting order");
        long now = System.nanoTime();
        events = getEventsBetween("acct-1", startTime, endTime, null, 100_000, 
                SortDirection.ASCENDING, "device-acct-1-8", "device-acct-1-9", "device-acct-10");
        long totalTime = System.nanoTime() - now;
        System.out.printf("Time taken: %,dus\n", totalTime / 1000);
        displayEvents(events);
        
    }
    
    // ============================================================================
    // MAIN METHOD FOR DEMONSTRATION
    // ============================================================================
    
    /**
     * Main method demonstrating the usage of the Event management system.
     * 
     * @param args Command line arguments (not used)
     * @throws Exception if any error occurs during execution
     */
    public static void main(String[] args) throws Exception {
        try (TimeSeriesDemo runner = createWithDefaultClient()) {
            // Generate sample data
            runner.generateSampleData();
            
            // Demonstrate queries
            runner.demonstrateQueries();
        }
    }
}
