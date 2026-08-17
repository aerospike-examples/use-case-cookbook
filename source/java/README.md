# Use Case Cookbook — Java (Legacy Client)

This is the original Java implementation of the [Use Case Cookbook](../../README.md), built on
Aerospike's legacy Java client, the [Java Object Mapper](https://github.com/aerospike/java-object-mapper),
and the [Java Object Generator](https://github.com/aerospike-examples/java-object-generator).

For the same use cases against Aerospike's new Java SDK (currently alpha), see
[`../java-sdk`](../java-sdk/README.md).

## Setup

It is assumed that you have access to a running Aerospike cluster to work through these examples.
If you don't, there are numerous ways to obtain one, including following
[this blog](https://aerospike.com/blog/community-edition-aerolab/). No instruction on setting up
Aerospike clusters is provided here.

Build the jar first:
```
cd source/java
mvn clean package
```

To connect to your database, specify a seed node as an argument to the program. If no seed node
is passed, a database running on `localhost:3000` is assumed.
```
java -jar target/use-case-cookbook-0.8.0-full.jar -h localhost:3100
```

There are many connection options supported for different cluster configurations — use `--help`
on the command line to see the usage.

The namespace used by the demos defaults to `test`, but can be changed with the system property
`-Ddemo.namespace=<name>`.

## Data modeling libraries

Since we're talking about use case modeling, business objects (POJOs) are used to reflect
real-world programming in most of the use cases. Besides the Aerospike client library, three
additional projects make the code easier to read:

1. [**Project Lombok:**](https://projectlombok.org/) provides easy ways to define business
   objects without needing to write all the boilerplate code (getters, setters, constructors)
   normally associated with Java POJOs. For example, a simple Account class including getters,
   setters and constructors could look like:
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

2. [**Aerospike's Java Object Mapper:**](https://github.com/aerospike/java-object-mapper) allows
   business objects to be mapped directly to the Aerospike database through annotations. Most use
   cases intentionally also show raw client operations (bins, keys, expressions) side-by-side for
   teaching purposes, but for tasks like seeding data the mapper reduces boilerplate significantly.

3. [**Aerospike's Java Object Generator:**](https://github.com/aerospike-examples/java-object-generator)
   quickly and easily seeds POJOs with plausible values. This library isn't published to Maven
   Central, so you need to clone and install it locally first:

    ```
    git clone https://github.com/aerospike-examples/java-object-generator
    cd java-object-generator
    mvn clean package
    mvn install:install-file -Dfile=target/java-object-generator-0.9.0.jar -DgroupId=com.aerospike -DartifactId=java-object-generator -Dversion=0.9.0 -Dpackaging=jar
    ```

### Why these three work well together

Suppose we want to generate 10,000 different accounts with meaningful data and store them in
Aerospike.

First, create a data model:
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
`@AerospikeRecord` tells the Java Object Mapper to save Accounts to the `uccb_account` set in the
`test` namespace, and `@AerospikeKey` marks `id` as the record's primary key.

`@GenMagic` tells the generator to take its best guess at populating every field. This can be
overridden if you want specific value ranges, but the defaults are a reasonable starting point.

Then, to create and save the objects:
```java
AeroMapper mapper = new AeroMapper.Builder(client).build();
new Generator(Account.class)
    .generate(1, 10_000, 0, Account.class, mapper::save)
    .monitor();
```

The first line creates a mapper to save objects into Aerospike. The second creates a generator and
uses it to generate ids from 1 to 10,000 — the `0` means "use as many threads as there are
processors" so it runs efficiently. For each account generated, the mapper saves it into
Aerospike. The final `.monitor()` call starts a progress monitor and returns when generation
completes:
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

If you wanted account balances within a range like $500 to $20,000, just change the field
definition:
```java
@GenNumber(start = 50000, end = 2000000)
private int balanceInCents;
```

Note that set names are prefixed with `uccb_` (short for "Use Case CookBook") so they don't
conflict with any other data you might have in the namespace. The demos truncate their sets before
seeding, so this also protects unrelated data from being wiped.

## Running the examples

A single entry point is provided with a menu to select the use case to run:
`com.aerospike.examples.UseCaseCookbookRunner`. It presents a menu similar to:

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

Select a use case by number; it will execute, present results, and then let you select another.
The menu also supports search — see [README_SEARCH.md](README_SEARCH.md) for the `search`/`/regex`
commands.

You can also run a single use case non-interactively with `-uc <name>` (partial name match
allowed) — see `--help` for this and other batch-mode flags (`-l`/`--listUseCases`,
`-ro`/`--runOnly`, `-so`/`--seedOnly`, `--param.<name>=<value>`).

Several use cases rely on ACID transactions to ensure data correctness. Aerospike introduced this
in version 8, and it requires the `test` namespace to be configured with `strong-consistency`
(an enterprise feature). To let these demonstrations run on any cluster, the version/config is
checked at startup — if the cluster doesn't meet that bar, a warning is issued and a shim is
inserted into the client that silently ignores the transactional aspects.

## Contributing a new use case

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for the full walkthrough.
