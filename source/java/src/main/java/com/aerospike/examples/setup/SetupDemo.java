package com.aerospike.examples.setup;

import java.util.Date;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Record;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.setup.model.Account;
import com.aerospike.generator.Generator;
import com.aerospike.mapper.tools.AeroMapper;

public class SetupDemo implements UseCase {
    @Override
    public String getName() {
        return "Demo setup";
    }

    @Override
    public String getDescription() {
        return "First application to make sure your environment is set up correctly. Inserts some Accounts and reads the data back";
    }
    

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/setup.md";
    }
    
    @Override
    public void setup(IAerospikeClient client, AeroMapper mapper) throws Exception {
        client.truncate(null, mapper.getNamespace(Account.class), mapper.getSet(Account.class), null);
        
        new Generator(Account.class)
            .generate(1, 10_000, 0, Account.class, account -> {
                mapper.save(account);
            })
            .monitor();
    }

    @Override
    public void run(IAerospikeClient client, AeroMapper mapper) throws Exception {
        String namespace = mapper.getNamespace(Account.class);
        String setName = mapper.getSet(Account.class);

        System.out.println("Query first 100 accounts");
        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(setName);
        stmt.setMaxRecords(100);
        
        try (RecordSet recordSet = client.query(null, stmt)) {
            while (recordSet.next()) {
                Record record = recordSet.getRecord();
                System.out.printf("Id: %s, Account Name: %s, Balance $%.02f, Date Opened: %s\n",
                        record.getString("id"),
                        record.getString("accountName"),
                        record.getLong("balanceInCents")/100.0,
                        new Date(record.getLong("dateOpened"))
                    );
            }
        }
    }
}
