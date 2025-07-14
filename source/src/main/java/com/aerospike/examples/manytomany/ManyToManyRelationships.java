package com.aerospike.examples.manytomany;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.aerospike.client.BatchResults;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.Utils;
import com.aerospike.examples.manytomany.model.Account;
import com.aerospike.examples.manytomany.model.Customer;
import com.aerospike.generator.Generator;
import com.aerospike.mapper.tools.AeroMapper;

public class ManyToManyRelationships implements UseCase{
    private static final int NUM_ACCOUNTS = 2_000;
    private static final int NUM_CUSTOMERS = 1_000;

    @Override
    public String getName() {
        return "Many to many relationships";
    }

    @Override
    public String getDescription() {
        return "Demonstrate how to handle manu-to-many relationships in Aerospike. Traversing relationships in both directions and adding "
                + "entites are discussed. See "
                + "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/many -to-many-relationships.md for details";
    }

    @Override
    public void setup(IAerospikeClient client, AeroMapper mapper) throws Exception {
        client.truncate(null, mapper.getNamespace(Customer.class), mapper.getSet(Customer.class), null);
        client.truncate(null, mapper.getNamespace(Account.class), mapper.getSet(Account.class), null);
        
        System.out.println("Generating Customers");
        new Generator(Customer.class)
            .generate(1, NUM_CUSTOMERS, Customer.class, mapper::save)
            .monitor();
        
        System.out.println("\nGenerating Accounts");
        new Generator(Account.class)
            .generate(1, NUM_ACCOUNTS, Account.class, account -> {
                int numAccountOwners = ThreadLocalRandom.current().nextInt(1,8);
                // Generate a list of the account owners
                List<String> ownerIds = IntStream.range(1, numAccountOwners+1)
                        .mapToObj(num -> "Cust-" + (ThreadLocalRandom.current().nextInt(NUM_CUSTOMERS) +1))
                        .collect(Collectors.toList());
                ownerIds.sort(null);
                
                addAccount(client, mapper, account, ownerIds);
            })
            .monitor();
    }
    
    /**
     * Add a new account to the database. This will insert the values into the database and 
     * update all the customers who are listed as owners of this account to include the ownershipe
     * @param client - The Aerospike client instance
     * @param mapper - The AeroMapper instance for getting namespace and set information
     * @param account - The Account to save
     * @param ownerIds - The list owner ids for this account
     * @return true if all oeprations were successful, false otherwise.
     */
    public boolean addAccount(IAerospikeClient client, AeroMapper mapper, Account account, List<String> ownerIds) {
        Key accountKey = new Key(mapper.getNamespace(Account.class), mapper.getSet(Account.class), account.getId().toString());
        
        return Utils.doInTransaction(client, txn -> {
            WritePolicy writePolicy = client.copyWritePolicyDefault();
            writePolicy.txn = txn;
            
            client.put(writePolicy, accountKey, 
                   new Bin("id", account.getId().toString()),
                   new Bin("accountName", account.getAccountName()),
                   new Bin("balanceInCents", account.getBalanceInCents()),
                   new Bin("dateOpened", account.getDateOpened() == null ? 0 : account.getDateOpened().getTime()),
                   new Bin("owners", ownerIds));
           
            BatchPolicy batchPolicy = client.copyBatchPolicyDefault();
            batchPolicy.txn = txn;
            
            // For each owning customer, we want to insert into  the `accounts` list using set-like behavior
            String customerNamespace = mapper.getNamespace(Customer.class);
            String customerSet = mapper.getSet(Customer.class);

            Key[] keys = ownerIds.stream()
                    .map(id -> new Key(customerNamespace, customerSet, id))
                    .toArray(Key[]::new);
            
            ListPolicy setLikeListPolicy = new ListPolicy(ListOrder.ORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);
            batchPolicy.txn = null;
            BatchResults results = client.operate(batchPolicy, null, keys, Operation.array(
                    ListOperation.append(setLikeListPolicy, "accounts", Value.get(account.getId().toString()))
                ));
            
            return results.status;
        });
    }

    /**
     * Determine any customers that the passed customer is related to via an account. That is, two customers are
     * related to each other if they have at least one account which they are both owners of.
     * @param client - The Aerospike client instance
     * @param mapper - The AeroMapper instance for getting namespace and set information
     * @param customerId - The customer id to find all related customers of.
     * @return - a Map of CustomerId -> Count, where count is the number of customers the accounts have in common.
     */
    public Map<String, Integer> getRelatedCustomers(IAerospikeClient client, AeroMapper mapper, String customerId) {
        Key customerKey = new Key(mapper.getNamespace(Customer.class), mapper.getSet(Customer.class), customerId);
        String accountNs = mapper.getNamespace(Account.class);
        String accountSet = mapper.getSet(Account.class);
        
        Record record = client.get(null, customerKey, "accounts");
        if (record != null) {
            List<String> accountIds = (List<String>) record.getList("accounts");
            Key[] accountKeys = accountIds.stream()
                    .map(id -> new Key(accountNs, accountSet, id))
                    .toArray(Key[]::new);
            
            Record[] records = client.get(null, accountKeys, "owners");
            
            // Now use Java streams to process into the map, extracting elements out of the 
            // lists of each record
            List<Record> recordList = Arrays.asList(records);
            Map<String, Integer> counts = recordList.stream()
                    .flatMap(rec -> {
                        List<String> list = (List<String>) rec.getList("owners");
                        return list == null ? Stream.empty() : list.stream();
                    })
                    .filter(id -> !customerId.equals(id))
                    .collect(Collectors.groupingBy(
                            Function.identity(),
                            Collectors.reducing(0, e->1, Integer::sum)
                    ));
            
            return counts;
        }
        return Map.of(); 
    }
    
    /**
     * Get a list of accounts related to the passed customer id.
     * @param client - The Aerospike client instance
     * @param mapper - The AeroMapper instance for getting namespace and set information
     * @param customerId - The customer id to find all related accounts of.
     * @return - a list of accounts related to this customer, or null if the customer does not exist.
     */
    public List<String> getRelatedAccountIds(IAerospikeClient client, AeroMapper mapper, String customerId) {
        Key customerKey = new Key(mapper.getNamespace(Customer.class), mapper.getSet(Customer.class), customerId);
        Record record = client.get(null, customerKey);
        if (record != null) {
            return (List<String>) record.getList("accounts");
        }
        return null;
    }
    
    /**
     * Get a list of accounts related to the passed customer id.
     * @param client - The Aerospike client instance
     * @param mapper - The AeroMapper instance for getting namespace and set information
     * @param customerId - The customer id to find all related accounts of.
     * throws - a list of accounts related to this customer, or null if the customer does not exist.
     */
    public void removeAssociation(IAerospikeClient client, AeroMapper mapper, String customerId, String accountId) {
        Key customerKey = new Key(mapper.getNamespace(Customer.class), mapper.getSet(Customer.class), customerId);
        Key accountKey = new Key(mapper.getNamespace(Account.class), mapper.getSet(Account.class), accountId);
        
        // Remove the customerId from the account object, remove the account id from the customer object
        Utils.doInTransaction(client, txn -> {
           WritePolicy writePolicy = client.copyWritePolicyDefault();
           writePolicy.txn = txn;
           
           // Remove the accountid from the customer
           Record rec = client.operate(writePolicy, customerKey, ListOperation.removeByValue("accounts", Value.get(accountId), ListReturnType.EXISTS));
           if (rec.getBoolean("accounts")) {
               // Remove the recordid from the account
               rec = client.operate(writePolicy, accountKey, ListOperation.removeByValue("owners", Value.get(customerId), ListReturnType.EXISTS));
               if (!rec.getBoolean("owners")) {
                   
                   throw new IllegalStateException(String.format(
                           "Account record for key '%s', should contain customer id '%s' in it's owners list, but does not",
                           accountId, customerId));
                   
               }
           }
           else {
               throw new IllegalStateException(String.format(
                       "Customer record for key '%s', should contain account id '%s' in it's account list, but does not",
                       customerId, accountId));
           }
        });
    }
    
    /**
     * Show the map of customer relationships on the terminal.
     * @param relationships
     */
    public void displayRelatedCustomers(Map<String, Integer> relationships) {
        System.out.println(" Customer | Count");
        System.out.println("----------+------");
        for (Entry<String, Integer> entry : relationships.entrySet()) {
            System.out.printf("%9s | %,3d\n", entry.getKey(), entry.getValue());
        }
    }
    @Override
    public void run(IAerospikeClient client, AeroMapper mapper) throws Exception {
        // Find all the people related to "Cust-1"
        Map<String, Integer> result = getRelatedCustomers(client, mapper, "Cust-1");
        System.out.printf("\nFinding all the customer related to cusomter 'Cust-1' (%d):\n", result.size());
        displayRelatedCustomers(result);
        
        List<String> accounts = getRelatedAccountIds(client, mapper, "Cust-1");
        if (accounts != null && !accounts.isEmpty()) {
            // This should always be the case
            System.out.printf("\nRemoving association between customer 'Cust-1' and account '%s'\n", accounts.get(0));
            removeAssociation(client, mapper, "Cust-1", accounts.get(0));
            
            result = getRelatedCustomers(client, mapper, "Cust-1");
            System.out.printf("\nRelationships after association was removed (%,d):\n", result.size());
            displayRelatedCustomers(result);
            
        }
    }

}
