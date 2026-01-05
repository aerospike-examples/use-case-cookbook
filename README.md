# use-case-cookbook
This repository is designed to showcase various aspects of data modeling with Aerospike through the use of practical examples involving use cases. 

The design patterns discussed will be pertinent to any programming language in which Aerospike has a [supported client API](https://aerospike.com/docs/develop/client-matrix/), however, the examples will be written in Java.

## Setup
It is assumed that you have access to a running Aerospike cluster to work through these examples. If you don't, there are numerous ways to obtain one, including following [this blog](https://aerospike.com/blog/community-edition-aerolab/). No instruction on setting up Aerospike clusters will be provided here.

To connect to your database, specify a seed node as an argument to the program. If no seed node is passed, a database running on `localhost:3000` will be assumed.
```
java -jar use-case-cookbook-0.8.0-full.jar -h localhost:3100
```

There are many connection options supported for different repository configurations -- use `--help` on the command like to see the usage.

Since we will be talking about use case modeling here, we will use business objects (POJOs) to reflect real-world programming in a lot of the use cases. Besides the Aerospike library, we will include three additional projects to make the code easier to read:

1. [**Project Lombok:**](https://projectlombok.org/) This excellent library provides easy ways to define business objects without needing to provide all the boilerplate code like getters and setters which are normally associated with Java POJOs. For example, a simple Account class including getters, setters and constrcutors could look like:
    ```java
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public class Account {
        private UUID id;
        private String accountName;
        private int balanceInCents;
        private Date dateOpened;
    }
    ```

2. [**Aerospike's Java Object Mapper:**](https://github.com/aerospike/java-object-mapper) This allows business objects to be mapped directly to the Aerospike database through annotations. We won't be using this in the recipes to show how to handle these situations in Java directly, however for tasks like seeding data it will reduce the boilderplate code significantly

3. [**Aerosike's Java Object Generator:**](https://github.com/aerospike-examples/java-object-generator) This library is designed to quickly and easily seed POJOs with values, which hopefully make sense. Note that this library is not currently in Maven, so to use it you would need to clone the repo and install in your local maven repository:

    ```
    git clone https://github.com/aerospike-examples/java-object-generator
    cd java-object-generator
    mvn clean package
    mvn install:install-file -Dfile=target/java-object-generator-0.9.0.jar -DgroupId=com.aerospike -DartifactId=java-object-generator -Dverion=0.9.0 -Dpackaging=jar
    ```

Let's take a look at why these three libraries will work really well together. Suppose we want to generate 10,000 different accounts with meaningful data and store them in Aerospike.

First step is to create a data model:
```java
@Data
@NoArgsConstructor
@AllArgsConstructor
@GenMagic
@AerospikeRecord(namespace = "test", set = "uccb_account")
public class Account {
    @AerospikeKey
    private UUID id;
    private String accountName;
    private int balanceInCents;
    private Date dateOpened;
}
```
The `@AerospikeRecord` tell the Java object mapper that it should save Accounts to the `account` set in the `test` namespace, and the `@AerospikeKey` annotation marks the `id` field as the primary key for the record.

The `@GenMagic` annotation tells the generator to take its best guess at populating values into all the fields. This can be overriden if you want specific values ranges, but for now we'll just leave it as the defaults.

Then, to create and save the objects, you can use:
```java
AeroMapper mapper = new AeroMapper.Builder(client).build();
new Generator(Account.class)
    .generate(1, 10_000, 0, Account.class, maper::save)
    .monitor();
```

The first line creates a mapper to save objects into Aerospike. The second line creates a generator, then uses it to generate ids from 1 to 10,000. The `0` says to use as many threads as there are processors so it runs efficiently. 

For each account that is created, the mapper is used to save it into Aerospike. 

The final `.monitor()` call starts off a monitor so you can see progress and the call will terminate when generation is complete.

This will create output similar to:
```
[1,028ms] 1,420 successful, 0 failed, 14.2% done 
[2,163ms] 10,000 successful, 0 failed, 100.0% done 
```

Now if you run AQL you should see the records there:
```
aql> select * from test.uccb_account
+-----------------------------------+----------------+---------------+----------------------------------------+
| accountName                       | balanceInCents | dateOpened    | id                                     |
+-----------------------------------+----------------+---------------+----------------------------------------+
| "Estefana Ruecker's account"      | 2797           | 1675746636688 | "e16d8ad7-189e-4db8-89fc-d004873c1ae8" |
| "Dr. Arnoldo MacGyver's account"  | 16544          | 1623953282143 | "ce02fb64-7d72-4303-8565-789c22c56c18" |
| "Jacquelyne Willms' account"      | 60960          | 1659545408842 | "062558c2-6472-46b9-bdab-42166c181f8d" |
| "Harris Jones' account"           | 80941          | 1712325468546 | "75b65b17-6dd1-48de-a2fb-b4c4dc901d20" |
| "Ardelia Renner's account"        | 97460          | 1725100495758 | "09703fc6-ee5c-4a52-b4c3-63330de7cd8e" |
| "Mr. Katia Kub's account"         | 73294          | 1626309869169 | "bb386963-cc2a-4784-8c6d-b9a0493d78bf" |
| "Amanda Crona's account"          | 4814           | 1624704844905 | "198aceb8-3735-4191-ac4f-4aa08d7d0975" |
```

Great! We have generated 10,000 records. If you wanted to make the account balances within a range like $500 to $20,000, you could just change the object definition like:
```java
@GenNumber(start = 50000, end = 2000000)
private int balanceInCents;
```
Note that the set name is prefixed with "uccb" which is short for "Use Case CookBook". The demos truncate the sets before using them, so having a longer name which will not conflict with any other data you might have is a good idea!

Now setup is complete and you can run the examples, explore the use cases!

# Use Cases
There are multiple use cases in this repository, each with a detailed explanation and sample code. In order to run these conveniently, a single entry point is provided with a menu to select the use case to run. This menu is is `com.aerospike.examples.Runner.java` and presents a menu similar to:

```
------------------------------------------------------------------------------------------------------------------------
|  No.  |         Use Case          |                                   Description                                    |
------------------------------------------------------------------------------------------------------------------------
|     1 | Demo setup                | First application to make sure your environment is set up correctly. Inserts     |
|       |                           | some Accounts and reads the data back                                            |
|     2 | One to many relationships | Demonstrate how to handle one-to-many relationships in Aerospike. Both being     |
|       |                           | able to query only from the parent to the child, and being able to query from    |
|       |                           | the child to the parent as well, are discussed.                                  |
```
A use case can be selected from the menu which will execute, present results and then allow you to select another use case.

Several of the use cases in this repository rely on ACID transactions to ensure correctness of data in all situations. Aerospike released this feature in version 8, so to run with transactions the database must be at v8+ and the namespace `test` must be configured to use `strong-consistency` which is an enterprise feature.

To allow these demonstrations to be executed by everyone, the system will detect if you're using a database version before version 8, or your `test` namespace is not configured for strong consistency. If either of these conditions is true, a warning will issued and a shim inserted into the Aerospike client library to silently ignore the transactional aspects.

Note that the namespace can be altered by setting a system property `-Ddemo.namespace=<name>`, but will default to `test`.

## Managing Top level objects in a one-to-many relationship
Situations where there are two related entities which are associated with one another, with one entity having many instances of the other entity. Both entities have business value in their own right, so one cannot be aggregated (nested) inside the other. For example a Department has Employees, and each Employee belongs to exactly one department. See [Managing One to Many relationships](UseCases/one-to-many-relationships.md)

## Managing Top level objects in a many-to-many relationship
Situations where there are two related entities which are associated with one another, with each entity having many instances of the other entity. Both entities have business value in their own right. For example a bank Customer can have multiple Accounts, but each Account can be owned by multiple Customers. See [Managing Many to Many relationships](UseCases/many-to-many-relationships.md)

## Leaderboards
Competitive gamers want to have games which are challenging but winnable, and want to know where the stand compared to other players. They aslo want to feel like they're progressing in the game. These criteria need leaderboards to work successfully, and being able to do these at scale with millions or tens of millions of players, playing thousands of games a second creates significant challenges. See [Leaderboards](UseCases/leaderboard.md)

## Player Matching
Related to leaderboards is the ability to have players matching against similar level opponents with various criteria based on the game. We need to match opponents at scale, efficiently. See [Player Matching](UseCases/player-matching.md)

## Time Series Data
Inserting, updating and querying time-series data is very important in range of industries. From monitoring dashboards, to credit card swipes, to motion detects, many use cases have this requirement. See [Time Series](UseCases/timeseries.md)

## Time Series Data with Large Variance
In many cases, time series data is not regular, but rather has a bell-curve (normal) style distribution, with a large number of stimuli generating a small number of events, but a small number of stimuli causing a very large number of events. Think corporate credit card swipes, social media when disaster strikes, etc. This is a harder set of data to model efficiently for, this use case implements a pattern to do so. See [Time Series with Large Variance](UseCases/timeseries-large-variance.md)

## Recent events across DCs
It is not uncommon to need to merge events on the same account across different DCs. For example, a credit card company wants the 50 most recent transactions for a credit card, but transactions can be generated in either of two DCs. Eventual consistency is a must -- latency and throughput requirements do not allow a stretch cluster between the different DCs. See [Transactions across DCs](UseCases/top-transactions-across-dcs.md)

## Advanced Expressions
A collection of techniques showing advanced use of expressions. Some of these techniques are not intuitiely obvious, so this section is more of an education in what is possible, rather than targeting a specific use case. See [Advanced Expressions](UseCases/advanced-expressions.md)

# Contributing

## Adding a New Use Case
Want to contribute your own use case to this cookbook? We'd love to have your contribution! See our comprehensive guide on [How to Add a New Use Case](CONTRIBUTING.md) which includes:

- Step-by-step instructions for creating a use case
- Complete, working example with ~15 fields and @GenMagic
- How to use the Async library (`runFor`, `periodic`, `continuous`)
- Using Parameters instead of hard-coded constants
- Data model best practices
- Documentation guidelines
- AQL query examples
- End-to-end testing procedures

The guide walks you through a fully worked example of an Inventory Management System that demonstrates all the patterns and best practices used in this repository.
