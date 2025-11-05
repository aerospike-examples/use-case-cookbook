package com.aerospike.examples.timeseries;

import java.time.LocalDate;
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractMap;
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
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.Utils;
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
public class TimeSeriesLargeVarianceDemo implements UseCase, AutoCloseable {
    
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
        public static final String CONTINUATION_BIN = "cont";
        public static final int MAX_RECORDS_PER_BUCKET = 10;
        public static final int PERCENT_EVENTS_IN_ORIG_BUCKET = 80;
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
        public static final int NUM_ACCOUNTS = 100;
        public static final int MAX_DEVICES_PER_ACCOUNT = 8;
        public static final int MAX_EVENTS_PER_DEVICE = 20;
        public static final String DEFAULT_VIDEO_URL = "https://somewhere.com/4659278373492";
        public static final String DEFAULT_STORAGE_LOCATION = "hv";
        public static final int NUM_EVENTS_ACCT_1 = 25_000;
        
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
    public TimeSeriesLargeVarianceDemo() {
        this.mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        this.eventCreator = new ValueCreator<>(Event.class);
    }
    
    /**
     * Creates a new Runner instance with a default Aerospike client configuration.
     * 
     * @return A configured Runner instance
     */
    public static TimeSeriesLargeVarianceDemo createWithDefaultClient() {
        ClientPolicy clientPolicy = createDefaultClientPolicy();
        IAerospikeClient client = new AerospikeClient(clientPolicy, DatabaseConfig.HOST, DatabaseConfig.PORT);
        AeroMapper mapper = new AeroMapper.Builder(client).build();
        return new TimeSeriesLargeVarianceDemo().setClient(client, mapper);
    }

    public TimeSeriesLargeVarianceDemo setClient(IAerospikeClient client, AeroMapper mapper) {
        this.client = client;
        DatabaseConfig.NAMESPACE = mapper.getNamespace(Event.class);
        DatabaseConfig.EVENT_SET = mapper.getSet(Event.class);
        return this;
    }
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
                + "This show a way to store time series data with events occurring on a sporadic (random) basis with "
                + "high variability in cardinality.";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/timeseries-large-variance.md";
    }
    
    @Override
    public String[] getTags() {
        return new String[] {"Map operations", "List Operations", "Nested CDT expressions", "Timeseries", "Expressions", "Adaptive", "Transactions"};
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
        setClient(client, mapper);
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
     * Calculates the lowest possible timestamp for a given bucket.
     * This is the inverse of getBucketOffset - it takes a bucket number and returns
     * the timestamp of the start of that bucket.
     * 
     * @param bucket - The bucket number
     * @return The lowest possible timestamp in milliseconds for that bucket
     */
    public static String getLowestPossibleEventForBucket(long bucket) {
        long lowestTimeStamp = TimeConfig.DATE_OFFSET_MILLIS + (bucket * TimeConfig.MILLIS_PER_HOUR * TimeConfig.BUCKET_WIDTH_HOURS);
        return eventIdFromTimestamp(lowestTimeStamp, true);
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
    
    public Exp getDoesContinuationBlockExistExp() {
        return Exp.binExists(DatabaseConfig.CONTINUATION_BIN);
    }
    
    private Key getContinuationKeyFromKey(Key key, String subKey) {
        return new Key(key.namespace, key.setName, key.userKey.toString() + "-" + subKey);
    }
    
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
        

        // There are 3 distinct case:
        // 1. The record only has a root block and this event will not make it overflow
        // 2. The record only has a root block and this event will make it overflow
        // 3. The root block has already split
        Exp canWriteToRootBlock = Exp.and(
                Exp.not(
                        getDoesContinuationBlockExistExp()
                ),
                Exp.lt(
                        MapExp.size(Exp.mapBin(DatabaseConfig.BIN_NAME)),
                        Exp.val(DatabaseConfig.MAX_RECORDS_PER_BUCKET)
                )
        );
        
        writePolicy.failOnFilteredOut = false;
        writePolicy.filterExp = Exp.build(canWriteToRootBlock);
        
        Record record = client.operate(writePolicy, key, 
                MapOperation.put(mapPolicy, DatabaseConfig.BIN_NAME, 
                    Value.get(event.getId()), 
                    Value.get(List.of(event.getDeviceId(), convertEventToMap(event)))
                )
            );
        
        if (record == null) {
            splitRootBlock(event, key);
        }
    }
    
    private void splitEventsIntoLists(Record record, List<Entry<String, List<?>>> majorityEvents, List<Entry<String, List<?>>> minorityEvents) {
        List<Entry<String, List<?>>> list = (List<Entry<String, List<?>>>) record.getList(DatabaseConfig.BIN_NAME);
        int threshold = list.size() * DatabaseConfig.PERCENT_EVENTS_IN_ORIG_BUCKET / 100;
        for (int i = 0; i < list.size(); i++) {
            if (i < threshold) {
                majorityEvents.add(list.get(i));
            }
            else {
                minorityEvents.add(list.get(i));
            }
        }
    }
    
    /**
     * This method is called when the root block overflows. In this case we need to:
     * <ol>
     * <li>Get all the entries from the root block</li>
     * <li>Write the continuation map with 2 entries: 
     * <ul>
     * <li>The smallest possible value for this block
     * <li>The next block, split at the
     * </ul>
     * <li>Write the records </li>
     * </ol>
     * Note that we need to validate that the block has not already split as part of the
     * transaction to prevent race conditions.
     * @param writePolicy
     * @param event
     */
    private void splitRootBlock(Event event, Key key) {
        Utils.doInTransaction(client, txn -> {
            WritePolicy writePolicy = client.copyWritePolicyDefault();
            writePolicy.txn = txn;
            Record record = client.operate(writePolicy, key,
                    // Get the map of events in the root block, if they exist
                    MapOperation.getByIndexRange(DatabaseConfig.BIN_NAME, 0, MapReturnType.KEY_VALUE),
                    // In case the root block has already split, get the continuation key closest to our event
                    ExpOperation.read("index", Exp.build(
                        Exp.cond(getDoesContinuationBlockExistExp(),
                            ListExp.getByValueRelativeRankRange(ListReturnType.VALUE, 
                                Exp.val(event.getId()), 
                                Exp.val(-1),
                                Exp.val(2),
                                Exp.listBin(DatabaseConfig.CONTINUATION_BIN)),
                            Exp.val(List.of(""))
                        )
                    )
                    , ExpReadFlags.DEFAULT));
            
            List<String> items = ((List<String>)record.getList("index"));
            String index = items.get(0);
            if (index.isEmpty()) {
                // Split this bin
                List<Entry<String, List<?>>> majorityEvents = new ArrayList<>();
                List<Entry<String, List<?>>> minorityEvents = new ArrayList<>();
                splitEventsIntoLists(record, majorityEvents, minorityEvents);
                
                long timestamp = event.getTimestamp().getTime();
                long bucket = getBucketOffset(timestamp);
                
                String minorityEventId = minorityEvents.get(0).getKey();
                String majorityEventId = getLowestPossibleEventForBucket(bucket);
                
                Key minorityKey = getContinuationKeyFromKey(key, minorityEventId);
                Key majorityKey = getContinuationKeyFromKey(key, majorityEventId);
                
                // Remove the data from the root block and populate the map
                String binName = DatabaseConfig.CONTINUATION_BIN;
                client.operate(writePolicy, key, 
                        //Operation.put(Bin.asNull(DatabaseConfig.BIN_NAME)),
                        MapOperation.clear(DatabaseConfig.BIN_NAME),
                        ListOperation.create(binName, ListOrder.ORDERED, true),
                        ListOperation.append(binName, Value.get(minorityEventId)),
                        ListOperation.append(binName, Value.get(majorityEventId)));
                
                // Write the events
                writePolicy.expiration = record.getTimeToLive();
                client.operate(writePolicy, minorityKey, 
                        Operation.put(new Bin(DatabaseConfig.BIN_NAME, minorityEvents, MapOrder.KEY_ORDERED)),
                        MapOperation.put(mapPolicy, DatabaseConfig.BIN_NAME, 
                                Value.get(event.getId()), 
                                Value.get(List.of(event.getDeviceId(), convertEventToMap(event)))));
                client.put(writePolicy, majorityKey, new Bin(DatabaseConfig.BIN_NAME, majorityEvents, MapOrder.KEY_ORDERED));
            }
            else {
                if (items.size() > 1) {
                    // Find the biggest element in the list less than or equal to our id.
                    for (int i = items.size() -1; i >= 0; i--) {
                        if (items.get(i).compareTo(event.getId()) <= 0) {
                            index = items.get(i);
                            break;
                        }
                    }
                }
                splitExists(writePolicy, event, key, index);
            }
            
        });
    }
    
    /**
     * The bucket has already split, and we know which sub-record to insert this event into. So insert it, but be
     * careful of overflowing the number of records allowed in this sub-record too. If we're going to overflow it,
     * split this bucket too. 
     * <p/>
     * In order to do this, there are 3 actions we need:
     * <ol>
     * <li>Insert the event into this sub-record, but only if it won't overflow the bucket</li>
     * <li>Get the events which will form the minority list for the next records if this bucket is overflowing</li>
     * <li>Remove the events which will go to the next record from this bucket, if it's overflowing</li>
     * </ol>
     * So for example, if we have 10 events max per buckets, and 80% of records get retained in the original bucket,
     * when it splits we will take 2 events out of this bucket and populate them in the next bucket, along with the
     * new event. 
     * <p/>
     * Note: Since the operations get applied in order, and points 1 and 3 affect the number of records in this bucket
     * and hence whether they would overflow or not, we need 2 different criteria. For example:
     * <ul>
     * <li>Assume there are 9 records in the bucket. Adding a 10th record will not cause overflow</li>
     * <li>However, after we add the record (10th item), steps 2 and 3 will believe that adding the record will cause
     * overflow (as there's now 10 records in the bucket) unless the overflow criteria is changed.</li>
     * <ul>
     * @param writePolicy
     * @param event
     * @param baseKey
     * @param index
     */
    private void splitExists(WritePolicy writePolicy, Event event, Key baseKey, String index) {
        // Insert the item into the record 
        Key splitKey = getContinuationKeyFromKey(baseKey, index);
        int minorSplitItems = DatabaseConfig.MAX_RECORDS_PER_BUCKET * (100 - DatabaseConfig.PERCENT_EVENTS_IN_ORIG_BUCKET) / 100;

        
        Exp willEventMakeBinOverflow = Exp.ge(
                MapExp.size(Exp.mapBin(DatabaseConfig.BIN_NAME)),
                Exp.val(DatabaseConfig.MAX_RECORDS_PER_BUCKET)
            );
 
        // If this event will cause the bin to overflow, read the minority events, which will be
        // moved to the next split of this bucket.
        Operation readMinorityEvents = ExpOperation.read("minority", Exp.build(
                    Exp.cond(
                        willEventMakeBinOverflow,
                        // This record will cause overflow, it must be split
                        MapExp.getByIndexRange(
                                MapReturnType.ORDERED_MAP, 
                                Exp.val(-minorSplitItems), 
                                Exp.mapBin(DatabaseConfig.BIN_NAME)),
                        
                        // This record will not cause overflow, return empty
                        Exp.val(Map.of())
                    )
                ),
                ExpReadFlags.DEFAULT);
        
        // If this event will cause the bin to overflow, remove the minority events as they will be
        // moved to the next split of this bucket.
        Operation removeMinorityEvents = ExpOperation.write(DatabaseConfig.BIN_NAME, Exp.build(
                Exp.cond(
                    willEventMakeBinOverflow,
                    // This record will cause overflow, it must be split
                    MapExp.removeByIndexRange(
                            MapReturnType.KEY_VALUE, 
                            Exp.val(-minorSplitItems), 
                            Exp.mapBin(DatabaseConfig.BIN_NAME)),
                    
                    // This record will not cause overflow, return empty
                    Exp.unknown()
                )
            ),
            ExpWriteFlags.EVAL_NO_FAIL);
    
        // If this event won't cause overflow, insert the record.
        Operation addEventToMapIfAllowed = ExpOperation.write(DatabaseConfig.BIN_NAME, Exp.build(
                Exp.cond(
                        willEventMakeBinOverflow,
                        Exp.unknown(),
                        MapExp.put(mapPolicy, 
                                Exp.val(event.getId()), 
                                Exp.val(List.of(event.getDeviceId(), convertEventToMap(event))),
                                Exp.mapBin(DatabaseConfig.BIN_NAME)
                        )
                )
            ),
            ExpWriteFlags.EVAL_NO_FAIL);
        
        Record record = client.operate(writePolicy, splitKey, 
                addEventToMapIfAllowed,
                readMinorityEvents,
                removeMinorityEvents);
        
        Map<String, List<?>> map = (Map<String, List<?>>) record.getMap("minority");
        if (map.size() > 0) {
            processOverflowOfSubRecord(writePolicy, map, baseKey, event);
        }
    }
    
    private void processOverflowOfSubRecord(WritePolicy writePolicy, Map<String, List<?>> map, Key baseKey, Event event) {
        // The bin overflowed, the list returned the extra records, smallest first.
        // Populate these in new bin and update the main record
        List<Entry<String, List<?>>> overflow = new ArrayList<>(map.entrySet());
        String newRecordKey = overflow.get(0).getKey();
        Key minorityKey = getContinuationKeyFromKey(baseKey, newRecordKey);
        client.operate(writePolicy, 
                minorityKey, 
                Operation.put(new Bin(DatabaseConfig.BIN_NAME, overflow, MapOrder.KEY_ORDERED)));
                MapOperation.put(mapPolicy, DatabaseConfig.BIN_NAME, 
                        Value.get(event.getId()), 
                        Value.get(List.of(event.getDeviceId(), convertEventToMap(event))));

        client.operate(writePolicy, baseKey, ListOperation.append(DatabaseConfig.CONTINUATION_BIN, Value.get(newRecordKey)));

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
        QueryRange queryRange = buildQueryRange(startTimestamp, endTimestamp, eventId, direction);
        
        Operation operation = createFilterOperation(queryRange.earliestEventId, queryRange.latestEventId, count, deviceIds);
        Operation getContinuationBlock = Operation.get(DatabaseConfig.CONTINUATION_BIN);

        // Note: for 100% consistent results, could do this in a transaction. But without
        // transactions you won't get read conflicts on quickly updating records, and will
        // not miss any updates
        processRecordsInRange(accountId, queryRange, operation, getContinuationBlock, count, results, direction);
        
        return results;
    }
    
    /**
     * Helper class to encapsulate query range information
     */
    private static class QueryRange {
        final String earliestEventId;
        final String latestEventId;
        final long startRecord;
        final long endRecord;
        
        QueryRange(String earliestEventId, String latestEventId, long startRecord, long endRecord) {
            this.earliestEventId = earliestEventId;
            this.latestEventId = latestEventId;
            this.startRecord = startRecord;
            this.endRecord = endRecord;
        }
    }
    
    /**
     * Builds the query range based on input parameters and sort direction
     */
    private QueryRange buildQueryRange(Long startTimestamp, Long endTimestamp, String eventId, SortDirection direction) {
        String latestEventId;
        String earliestEventId;
        
        if (eventId != null) {
            if (direction == SortDirection.ASCENDING) {
                // Run from AFTER event id up to endTimestamp, or now if not specified
                earliestEventId = generateNextEventId(eventId);
                latestEventId = eventIdFromTimestamp(getLatestTimestamp(endTimestamp), false);
            } 
            else {
                // Run from startTimestamp, or now - 14 days, to the eventId. Note that as the
                // end event is exclusive, we do not need to calculate a prior event id.
                latestEventId = eventId;
                earliestEventId = eventIdFromTimestamp(getOldestTimestamp(startTimestamp), true);
            }
        } else {
            // Full date range, don't care about ascending or descending
            latestEventId = eventIdFromTimestamp(getLatestTimestamp(endTimestamp), false);
            earliestEventId = eventIdFromTimestamp(getOldestTimestamp(startTimestamp), true);
        }
        
        long startRecord = getBucketOffset(extractTimestampFromEventId(earliestEventId));
        long endRecord = getBucketOffset(extractTimestampFromEventId(latestEventId));
        
        return new QueryRange(earliestEventId, latestEventId, startRecord, endRecord);
    }
    
    /**
     * Processes records in the specified range, handling both ascending and descending order
     */
    private void processRecordsInRange(String accountId, QueryRange queryRange, Operation operation, 
            Operation getContinuationBlock, int count, List<Event> results, SortDirection direction) {
        
        if (direction == SortDirection.ASCENDING) {
            processRecordsAscending(accountId, queryRange, operation, getContinuationBlock, count, results);
        } 
        else {
            processRecordsDescending(accountId, queryRange, operation, getContinuationBlock, count, results);
        }
    }
    
    /**
     * Processes records in ascending order
     */
    private void processRecordsAscending(String accountId, QueryRange queryRange, Operation operation, 
            Operation getContinuationBlock, int count, List<Event> results) {
        
        for (long recordKey = queryRange.startRecord; 
                results.size() < count && recordKey <= queryRange.endRecord; 
                recordKey++) {
            
            Key key = new Key(DatabaseConfig.NAMESPACE, DatabaseConfig.EVENT_SET, accountId + ":" + recordKey);
            Record record = client.operate(null, key, operation, getContinuationBlock);
            
            if (record != null) {
                processRecordWithContinuations(record, key, operation, queryRange.latestEventId, count, results, SortDirection.ASCENDING);
            }
        }
    }
    
    /**
     * Processes records in descending order
     */
    private void processRecordsDescending(String accountId, QueryRange queryRange, Operation operation, 
            Operation getContinuationBlock, int count, List<Event> results) {
        
        for (long recordKey = queryRange.endRecord; 
                results.size() < count && recordKey >= queryRange.startRecord; 
                recordKey--) {
            
            Key key = new Key(DatabaseConfig.NAMESPACE, DatabaseConfig.EVENT_SET, accountId + ":" + recordKey);
            Record record = client.operate(null, key, operation, getContinuationBlock);
            
            if (record != null) {
                processRecordWithContinuations(record, key, operation, queryRange.earliestEventId, count, results, SortDirection.DESCENDING);
            }
        }
    }
    
    /**
     * Processes a record, handling continuation bins if they exist
     */
    private void processRecordWithContinuations(Record record, Key key, Operation operation, 
            String boundaryEventId, int count, List<Event> results, SortDirection direction) {
        
        List<String> continuationBin = (List<String>) record.getList(DatabaseConfig.CONTINUATION_BIN);
        
        if (continuationBin != null) {
            processContinuationBins(continuationBin, key, operation, boundaryEventId, count, results, direction);
        } 
        else {
            addEventsToResults(count, record, results, direction);
        }
    }
    
    /**
     * Processes continuation bins in the appropriate order
     */
    private void processContinuationBins(List<String> continuationBin, Key key, Operation operation, 
            String boundaryEventId, int count, List<Event> results, SortDirection direction) {
        
        if (direction == SortDirection.ASCENDING) {
            processContinuationBinsAscending(continuationBin, key, operation, boundaryEventId, count, results);
        } 
        else {
            processContinuationBinsDescending(continuationBin, key, operation, boundaryEventId, count, results);
        }
    }
    
    /**
     * Processes continuation bins in ascending order
     */
    private void processContinuationBinsAscending(List<String> continuationBin, Key key, Operation operation, 
            String latestEventId, int count, List<Event> results) {
        
        // Iterate over sub-records so long as they can possibly contain results in the correct range.
        // The items in the list are guaranteed to be in ascending order
        for (int subKeyIndex = 0; subKeyIndex < continuationBin.size(); subKeyIndex++) {
            String subKey = continuationBin.get(subKeyIndex);
            if (subKey.compareTo(latestEventId) > 0 || results.size() >= count) {
                break;
            }
            Key subRecordKey = getContinuationKeyFromKey(key, subKey);
            Record subRecord = client.operate(null, subRecordKey, operation);
            addEventsToResults(count, subRecord, results, SortDirection.ASCENDING);
        }
    }
    
    /**
     * Processes continuation bins in descending order
     */
    private void processContinuationBinsDescending(List<String> continuationBin, Key key, Operation operation, 
            String earliestEventId, int count, List<Event> results) {
        
        for (int subKeyIndex = continuationBin.size() - 1; subKeyIndex >= 0; subKeyIndex--) {
            String subKey = continuationBin.get(subKeyIndex);
            Key subRecordKey = getContinuationKeyFromKey(key, subKey);
            Record subRecord = client.operate(null, subRecordKey, operation);
            addEventsToResults(count, subRecord, results, SortDirection.DESCENDING);
            
            // When descending, we must compare the subKey to the earliest event id AFTER loading
            // the record as it's possible that there are some events at the end of the record which
            // are still valid.
            if (subKey.compareTo(earliestEventId) < 0 || results.size() >= count) {
                break;
            }
        }
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
        Event event = eventCreator.createAndPopulate(Map.of("Key", 1));
        
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
         // generator threads to 2 to prevent this - one for the large account, one for the rest.
         new Generator().generate(1, GenerationConfig.NUM_ACCOUNTS, 2, Account.class, account -> {
             // Force account 1 to have at least 10 devices and lots of events for demonstration
             if ("acct-1".equals(account.getId())) {
                 account.setNumDevices(Math.max(10, account.getNumDevices()));
                 long now = new Date().getTime();
                 long timestamp = now - TimeUnit.DAYS.toMillis(14);
                 for (int i = 0; i < GenerationConfig.NUM_EVENTS_ACCT_1; i++) {
                     timestamp += ThreadLocalRandom.current().nextLong(100);
                     int deviceId = ThreadLocalRandom.current().nextInt(account.getNumDevices());
                     Event event = generateSampleEvent(account.getId(), 
                             "device-" + account.getId() + "-" + deviceId);
                     event.setTimestamp(new Date(timestamp));
                     int randomValue = Math.abs(ThreadLocalRandom.current().nextInt());
                     event.setId(String.format("%013d%012d", timestamp, randomValue));

                     upsertEvent(event, true);
                     eventsCreated.incrementAndGet();
                 }
             }
             else {
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
        while (events.size() == pageSize && pageCounter < 5) {
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

        System.out.println("Showing events for acct-2, all devices");
        events = getEventsBefore("acct-2", null, 20_000);
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
        try (TimeSeriesLargeVarianceDemo runner = createWithDefaultClient()) {
            // Generate sample data
            runner.generateSampleData();
            
            // Demonstrate queries
            runner.demonstrateQueries();
        }
    }
}
