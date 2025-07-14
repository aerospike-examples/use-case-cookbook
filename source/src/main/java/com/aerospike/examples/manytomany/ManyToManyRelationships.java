package com.aerospike.examples.manytomany;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
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

    public Map<String, Integer> getRelatedCustomers(IAerospikeClient client, AeroMapper mapper, String customerId) {
        Key customerKey = new Key(mapper.getNamespace(Customer.class), mapper.getSet(Customer.class), customerId);
        return Utils.doInTransaction(client, txn -> {
            Policy readPolicy = new Policy();
            readPolicy.txn = txn;
            Record record = client.get(readPolicy, customerKey, "accounts");
            if (record != null) {
                List<String> accountIds = (List<String>) record.getList("accounts");
                
            }
            return null; 
        });
    }
    @Override
    public void run(IAerospikeClient client, AeroMapper mapper) throws Exception {
        // TODO Auto-generated method stub
        
    }

}
