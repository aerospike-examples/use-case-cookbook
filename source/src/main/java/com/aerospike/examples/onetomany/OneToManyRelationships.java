package com.aerospike.examples.onetomany;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Txn;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.Utils;
import com.aerospike.examples.onetomany.model.Agent;
import com.aerospike.examples.onetomany.model.Listing;
import com.aerospike.generator.Generator;
import com.aerospike.generator.ValueCreator;
import com.aerospike.mapper.tools.AeroMapper;

/**
 * Demonstrates one-to-many relationships in Aerospike.
 * This class shows how to handle relationships where one agent can have multiple listings,
 * and how to perform bidirectional queries (from agent to listings and vice versa).
 */
public class OneToManyRelationships implements UseCase {
    private static final int NUM_LISTINGS = 5_000;
    private static final int NUM_AGENTS = 1_000;

    @Override
    public String getName() {
        return "One to many relationships";
    }

    @Override
    public String getDescription() {
        return "Demonstrate how to handle one-to-many relationships in Aerospike. Both being "
                + "able to query only from the parent to the child, and being able to query from the child to the parent as well, are discussed.";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/one-to-many-relationships.md";
    }

    /**
     * Associates a listing with an agent using a transaction to ensure data consistency.
     * This method adds the listing id to the agent's listings list and sets the agent ID on the listing.
     * 
     * @param client - The Aerospike client instance
     * @param mapper - The AeroMapper instance for getting namespace and set information
     * @param listingId - The id of the listing to associate
     * @param agentId - The id of the agent to associate the listing with
     */
    private void addListingToAgent(IAerospikeClient client, AeroMapper mapper, String listingId, long agentId) {
        // Get the keys
        Key agentKey = new Key(mapper.getNamespace(Agent.class), mapper.getSet(Agent.class), agentId);
        Key listingKey = new Key(mapper.getNamespace(Listing.class), mapper.getSet(Listing.class), listingId);
        
        while (true) {
            Txn txn = new Txn();
            WritePolicy wp = client.copyWritePolicyDefault();
            wp.txn = txn;

            try {
                // Update the agent to incorporate the listing id. Create the list if it doesn't exist. Note that we mimic SET behaviour instead of a list
                ListPolicy setLikeListPolicy = new ListPolicy(ListOrder.ORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);
                client.operate(wp, agentKey, 
                        ListOperation.append(setLikeListPolicy, "listings", Value.get(listingId)));
                
                // Update the listing to store the referring agent
                client.put(wp, listingKey, new Bin("agentId", agentId));
                client.commit(txn);
                break;
            }
            catch (AerospikeException ae) {
                client.abort(txn);
                switch (ae.getResultCode()) {
                case ResultCode.MRT_BLOCKED:
                case ResultCode.MRT_EXPIRED:
                case ResultCode.MRT_VERSION_MISMATCH:
                case ResultCode.TXN_FAILED:
                    // These errors are retryable
                    break;
                default:
                    throw ae;
                }
            }
            catch (Exception e) {
                client.abort(txn);
                throw e;
            }
        }
    }
    
    /**
     * Sets up the initial data for the one-to-many relationship demonstration.
     * 
     * @param client - The Aerospike client instance
     * @param mapper - The AeroMapper instance for getting namespace and set information
     */
    @Override
    public void setup(IAerospikeClient client, AeroMapper mapper) throws Exception {
        client.truncate(null, mapper.getNamespace(Agent.class), mapper.getSet(Agent.class), null);
        client.truncate(null, mapper.getNamespace(Listing.class), mapper.getSet(Listing.class), null);
        
        System.out.println("Generating Agents");
        new Generator(Agent.class)
            .generate(1, NUM_AGENTS, Agent.class, mapper::save)
            .monitor();
        
        System.out.println("\nGenerating Listings");
        new Generator(Listing.class)
            .generate(1, NUM_LISTINGS, Listing.class, mapper::save)
            .monitor();
        
        System.out.println("\nAssociating listings with agents");
        for (int i = 1; i <= NUM_LISTINGS; i++) {
            String listingId = "Listing-" + i;
            int agentId = ThreadLocalRandom.current().nextInt(NUM_AGENTS);
            addListingToAgent(client, mapper, listingId, agentId);
        }
    
    }

    /**
     * Adds a new listing to an agent using a transaction to ensure data consistency.
     * This method:
     * <ol>
     * <li> Sets the agent id on the listing object</li>
     * <li> Converts the listing to Aerospike bins</li>
     * <li> Saves the listing details</li>
     * <li> Updates the agent's listings to include the new listing id</li>
     * </ol>
     * 
     * @param client - The Aerospike client instance
     * @param mapper - The AeroMapper instance for getting namespace and set information, and to turn the listing into bins
     * @param agentId - The ID of the agent to add the listing to
     * @param listing - The listing object to add
     */
    public void addListing(IAerospikeClient client, AeroMapper mapper, long agentId, Listing listing) {
        // Set the agent id on the listing
        listing.setAgentId(agentId);
        
        // Turn the listing into a series of Aerospike bins. This can be done manually without a mapper.
        Map<String, Object> valueMap = mapper.getMappingConverter().convertToMap(listing);
        Bin[] bins = valueMap.entrySet().stream()
                .map(entry -> new Bin(entry.getKey(), Value.get(entry.getValue())))
                .toArray(Bin[]::new);

        // Get the keys to use
        Key agentKey = new Key(mapper.getNamespace(Agent.class), mapper.getSet(Agent.class), agentId);
        Key listingKey = new Key(mapper.getNamespace(Listing.class), mapper.getSet(Listing.class), listing.getId());

        Utils.doInTransaction(client, txn -> {
            WritePolicy writePolicy = client.copyWritePolicyDefault();
            writePolicy.txn = txn;
            
            // Save the listing details
            client.put(writePolicy, listingKey, bins);
            
            // Update the agent to incorporate the listing id. Create the list if it doesn't exist. Note that we mimic SET behaviour instead of a list
            ListPolicy setLikeListPolicy = new ListPolicy(ListOrder.ORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);
            client.operate(writePolicy, agentKey, 
                    ListOperation.append(setLikeListPolicy, "listings", Value.get(listing.getId())));
        });
    }
    
    /**
     * Deletes a listing and removes it from the associated agent's listings.
     * This method uses a transaction to ensure both operations succeed or fail together.
     * 
     * @param client - The Aerospike client instance
     * @param mapper - The AeroMapper instance for getting namespace and set information
     * @param listingId - The id of the listing to delete
     * @return true if the listing was successfully deleted, false if the listing was not found
     */
    public boolean deleteListing(IAerospikeClient client, AeroMapper mapper, String listingId) {
        Key listingKey = new Key(mapper.getNamespace(Listing.class), mapper.getSet(Listing.class), listingId);

        return Utils.doInTransaction(client, txn -> {
            WritePolicy writePolicy = client.copyWritePolicyDefault();
            writePolicy.txn = txn;
            writePolicy.durableDelete = true;
            
            // Get the agentId from the listing and delete the listing
            try {
                Record listing = client.operate(writePolicy, listingKey, 
                        Operation.get("agentId"),
                        Operation.delete());
                Key agentKey = new Key(mapper.getNamespace(Agent.class), mapper.getSet(Agent.class), listing.getLong("agentId"));
                // Remove the listing from the list on the agent
                Record result = client.operate(writePolicy, agentKey, ListOperation.removeByValue("listings", Value.get(listingId), ListReturnType.EXISTS));
                return result.getBoolean("listings");
            }
            catch (AerospikeException e) {
                if (e.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                    return false;
                }
                throw e;
            }
        });
    }
    
    /**
     * Retrieves all listings associated with a specific agent.
     * This method demonstrates how to query from the parent (agent) to children (listings).
     * It uses a batch read operation to efficiently retrieve multiple listings at once.
     * 
     * @param client - The Aerospike client instance
     * @param mapper - The AeroMapper instance for getting namespace and set information
     * @param agentId - The id of the agent whose listings to retrieve
     * @return A list of Listings associated with the specified agent id
     */
    public List<Listing> getListings(IAerospikeClient client, AeroMapper mapper, long agentId) {
        Key agentKey = new Key(mapper.getNamespace(Agent.class), mapper.getSet(Agent.class), agentId);
        return Utils.doInTransaction(client, txn -> {
            Policy readPolicy = client.copyReadPolicyDefault();
            readPolicy.txn = txn;
            // Read just the listing from the agent
            Record agent = client.get(readPolicy, agentKey, "listings");
            if (agent != null) {
                List<String> listingIds = (List<String>) agent.getList("listings");
                // Turn each Id into a key
                Key[] keys = listingIds.stream()
                        .map(listingId -> new Key(mapper.getNamespace(Listing.class), mapper.getSet(Listing.class), listingId))
                        .toArray(Key[]::new);
                
                // Perform a batch read to read all the listing
                BatchPolicy batchPolicy = client.getBatchPolicyDefault();
                batchPolicy.txn = txn;
                Record[] listings = client.get(batchPolicy, keys);
                
                List<Listing> results = new ArrayList<>();
                for (Record thisListing : listings) {
                    if (thisListing != null) {
                        // Note: This is the raw Aerospike way of doing it. If using an AeroMapper, could use 
                        // mapper.getMappingConverter().convertToObject(...) 
                        results.add(new Listing(
                                thisListing.getString("id"),
                                thisListing.getString("line1"),
                                thisListing.getString("line2"),
                                thisListing.getString("city"),
                                thisListing.getString("state"),
                                thisListing.getString("zipCode"),
                                thisListing.getString("url"),
                                thisListing.getLong("dateListed") == 0? null : new Date(thisListing.getLong("dateListed")),
                                thisListing.getLong("agentId"),
                                thisListing.getString("description")));
                    }
                }
                return results;
            }
            return List.of();
        });
    }
    
    /**
     * Runs the demonstration of one-to-many relationship operations.
     * This method demonstrates:
     * 1. Retrieving all listings for a random agent
     * 2. Adding a new listing to that agent
     * 3. Deleting a listing from that agent
     * 4. Displaying the results of each operation
     * 
     * @param client - The Aerospike client instance
     * @param mapper - The AeroMapper instance for object mapping
     */
    @Override
    public void run(IAerospikeClient client, AeroMapper mapper) throws Exception {
        long agentId = ThreadLocalRandom.current().nextInt(NUM_AGENTS) + 1;
        System.out.printf("Examining listings for agent %d:\n", agentId);
        List<Listing> listings = getListings(client, mapper, agentId);
        System.out.printf("\nCurrent listings (%,d): \n", listings.size());
        listings.forEach(listing -> System.out.println("   " + listing.toString()));
        
        ValueCreator<Listing> creator = new ValueCreator<>(Listing.class);
        Listing newListing = creator.createAndPopulate(Map.of("Key", "X999"));
        System.out.printf("\nAdding a new listing (%s)\n", newListing.getId());
        addListing(client, mapper, agentId, newListing);
        
        listings = getListings(client, mapper, agentId);
        System.out.printf("Listings after adding the new listing (%,d):\n", listings.size());
        listings.forEach(listing -> System.out.println("   " + listing.toString()));
        
        System.out.printf("\nDeleting Listing %s: %b\n", 
                listings.get(0).getId(), 
                deleteListing(client, mapper, listings.get(0).getId()));
        
        listings = getListings(client, mapper, agentId);
        System.out.printf("Listings after deleting a listing (%,d):\n", listings.size());
        listings.forEach(listing -> System.out.println("   " + listing.toString()));
    }

}
