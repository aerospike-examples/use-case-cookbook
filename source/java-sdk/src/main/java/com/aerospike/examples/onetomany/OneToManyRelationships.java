package com.aerospike.examples.onetomany;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.TypedDataSet;
import com.aerospike.client.sdk.TypedKey;
import com.aerospike.client.sdk.TypedKeyList;
import com.aerospike.client.sdk.TypedRecordStream;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.onetomany.model.Agent;
import com.aerospike.examples.onetomany.model.Listing;

/**
 * SDK port of the legacy {@code OneToManyRelationships} (see ../../java). Demonstrates a
 * one-to-many relationship: each agent holds a set-like list of listing ids, and each listing
 * stores the id of its owning agent, so the relationship can be queried from either side.
 * <p/>
 * The legacy version hand-rolls a transaction retry loop around MRT_BLOCKED/MRT_VERSION_MISMATCH/
 * TXN_FAILED; this port relies on {@link Session#doInTransaction}/{@link
 * Session#doInTransactionReturning}, which already retry those result codes internally.
 */
public class OneToManyRelationships implements UseCase {

    private static final int NUM_AGENTS = 1_000;
    private static final int NUM_LISTINGS = 5_000;

    private static final String[] FIRST_NAMES = {"Estefana", "Arnoldo", "Jacquelyne", "Harris", "Ardelia"};
    private static final String[] LAST_NAMES = {"Ruecker", "MacGyver", "Willms", "Jones", "Renner"};
    private static final String[] STREETS = {"Main St", "Oak Ave", "Elm St", "Maple Dr", "Cedar Ln"};
    private static final String[] CITIES = {"Springfield", "Franklin", "Greenville", "Clinton", "Fairview"};
    private static final String[] STATES = {"CA", "TX", "NY", "FL", "WA"};

    @Override
    public String getName() {
        return "One to many relationships";
    }

    @Override
    public String getDescription() {
        return "Demonstrate how to handle one-to-many relationships in Aerospike. Both being able to query "
                + "only from the parent to the child, and being able to query from the child to the parent as well, are discussed.";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/one-to-many-relationships.md";
    }

    private TypedDataSet<Agent> agents() {
        return TypedDataSet.of(System.getProperty("demo.namespace", "test"), "uccb_agent", Agent.class);
    }

    private TypedDataSet<Listing> listings() {
        return TypedDataSet.of(System.getProperty("demo.namespace", "test"), "uccb_listing", Listing.class);
    }

    private Agent randomAgent(long agentId) {
        String first = FIRST_NAMES[ThreadLocalRandom.current().nextInt(FIRST_NAMES.length)];
        String last = LAST_NAMES[ThreadLocalRandom.current().nextInt(LAST_NAMES.length)];
        long tenYearsMs = 10L * 365 * 24 * 60 * 60 * 1000;
        Date regDate = new Date(System.currentTimeMillis() - ThreadLocalRandom.current().nextLong(0, tenYearsMs));
        return new Agent(agentId, first, last,
                first.toLowerCase() + "." + last.toLowerCase() + "@example.com",
                String.format("555-%04d", ThreadLocalRandom.current().nextInt(10_000)),
                regDate);
    }

    private Listing randomListing(String id) {
        long oneYearMs = 365L * 24 * 60 * 60 * 1000;
        Date dateListed = new Date(System.currentTimeMillis() - ThreadLocalRandom.current().nextLong(0, oneYearMs));
        return new Listing(id,
                ThreadLocalRandom.current().nextInt(100, 9999) + " " + STREETS[ThreadLocalRandom.current().nextInt(STREETS.length)],
                "",
                CITIES[ThreadLocalRandom.current().nextInt(CITIES.length)],
                STATES[ThreadLocalRandom.current().nextInt(STATES.length)],
                String.format("%05d", ThreadLocalRandom.current().nextInt(100_000)),
                "https://example.com/listings/" + id,
                dateListed,
                0L,
                "A lovely property in a great neighborhood.");
    }

    private void addListingToAgent(Session session, String listingId, long agentId) {
        session.doInTransaction(tx -> {
            tx.upsert(agents().id(agentId))
                    .bin("listings").listAppend(listingId, opts -> opts.addUnique().allowFailures())
                    .execute();
            tx.upsert(listings().id(listingId))
                    .bin("agentId").setTo(agentId)
                    .execute();
        });
    }

    @Override
    public void setup(Session session) throws Exception {
        TypedDataSet<Agent> agents = agents();
        TypedDataSet<Listing> listings = listings();
        session.truncate(agents);
        session.truncate(listings);

        System.out.println("Generating Agents");
        for (long agentId = 1; agentId <= NUM_AGENTS; agentId++) {
            session.upsert(agents).object(randomAgent(agentId)).execute();
        }

        System.out.println("Generating Listings");
        for (int i = 1; i <= NUM_LISTINGS; i++) {
            session.upsert(listings).object(randomListing("Listing-" + i)).execute();
        }

        System.out.println("Associating listings with agents");
        for (int i = 1; i <= NUM_LISTINGS; i++) {
            String listingId = "Listing-" + i;
            long agentId = ThreadLocalRandom.current().nextInt(NUM_AGENTS) + 1;
            addListingToAgent(session, listingId, agentId);
        }
    }

    /**
     * Adds a new listing to an agent inside a transaction: saves the listing, then appends
     * its id to the agent's set-like {@code listings} list.
     */
    public void addListing(Session session, long agentId, Listing listing) {
        listing.setAgentId(agentId);
        TypedDataSet<Listing> listings = listings();
        session.doInTransaction(tx -> {
            tx.upsert(listings).object(listing).execute();
            tx.upsert(agents().id(agentId))
                    .bin("listings").listAppend(listing.getId(), opts -> opts.addUnique().allowFailures())
                    .execute();
        });
    }

    /**
     * Deletes a listing and removes it from its agent's {@code listings} list, inside a
     * transaction. Returns {@code false} if the listing did not exist.
     */
    public boolean deleteListing(Session session, String listingId) {
        TypedKey<Listing> listingKey = listings().id(listingId);
        return session.doInTransactionReturning(tx -> {
            Optional<RecordResult> existing = tx.query(listingKey).readingOnlyBins("agentId").execute().getFirst();
            if (existing.isEmpty() || !existing.get().isOk()) {
                return false;
            }
            long agentId = existing.get().recordOrThrow().getLong("agentId");
            tx.delete(listingKey).execute();

            RecordResult removeResult = tx.upsert(agents().id(agentId))
                    .bin("listings").onListValue(listingId).removeAnd().count()
                    .execute().getFirst().orElseThrow();
            return removeResult.recordOrThrow().getLong("listings") > 0;
        });
    }

    /**
     * Retrieves all listings for an agent: reads the agent's {@code listings} id list, then
     * batch-reads every listing referenced by it.
     */
    public List<Listing> getListings(Session session, long agentId) {
        TypedKey<Agent> agentKey = agents().id(agentId);

        return session.doInTransactionReturning(tx -> {
            Optional<RecordResult> agentResult = tx.query(agentKey).readingOnlyBins("listings").execute().getFirst();
            if (agentResult.isEmpty() || !agentResult.get().isOk()) {
                return List.<Listing>of();
            }
            Record agentRecord = agentResult.get().recordOrThrow();
            List<?> listingIds = agentRecord.getList("listings");
            if (listingIds == null || listingIds.isEmpty()) {
                return List.<Listing>of();
            }

            TypedKeyList<Listing> keys = new TypedKeyList<>();
            for (Object listingId : listingIds) {
                keys.add(listings().id((String) listingId));
            }

            List<Listing> results = new ArrayList<>();
            try (TypedRecordStream<Listing> stream = tx.query(keys).execute()) {
                stream.forEachObject(results::add);
            }
            return results;
        });
    }

    @Override
    public void run(Session session) throws Exception {
        long agentId = ThreadLocalRandom.current().nextInt(NUM_AGENTS) + 1;
        System.out.printf("Examining listings for agent %d:%n", agentId);
        List<Listing> listings = getListings(session, agentId);
        System.out.printf("%nCurrent listings (%,d):%n", listings.size());
        listings.forEach(listing -> System.out.println("   " + listing));

        Listing newListing = randomListing("Listing-X999");
        System.out.printf("%nAdding a new listing (%s)%n", newListing.getId());
        addListing(session, agentId, newListing);

        listings = getListings(session, agentId);
        System.out.printf("Listings after adding the new listing (%,d):%n", listings.size());
        listings.forEach(listing -> System.out.println("   " + listing));

        System.out.printf("%nDeleting Listing %s: %b%n",
                listings.get(0).getId(),
                deleteListing(session, listings.get(0).getId()));

        listings = getListings(session, agentId);
        System.out.printf("Listings after deleting a listing (%,d):%n", listings.size());
        listings.forEach(listing -> System.out.println("   " + listing));
    }
}
