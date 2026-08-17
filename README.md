# use-case-cookbook
This repository is designed to showcase various aspects of data modeling with Aerospike through the use of practical examples involving use cases.

The design patterns discussed are pertinent to any programming language in which Aerospike has a [supported client API](https://aerospike.com/docs/develop/client-matrix/). Each `UseCases/*.md` doc below describes a pattern in a language-agnostic way; the runnable code lives per-language under `source/`.

## Implementations

| Language / client | Location | Status |
| --- | --- | --- |
| Java (legacy client) | [`source/java`](source/java/README.md) | All use cases, interactive menu + search |
| Java (new SDK, alpha) | [`source/java-sdk`](source/java-sdk/README.md) | All use cases ported; CLI-only so far |

More languages (Go, Python legacy client, Python SDK) are planned — see [CLIENT-5234](https://aerospike.atlassian.net/browse/CLIENT-5234).

Each implementation seeds and reads the same namespace/set/bin names for a given use case, so the AQL examples in a `UseCases/*.md` doc stay accurate regardless of which language you ran to seed the data. Follow the linked README for the language you want to explore before working through the use cases below.

You'll need access to a running Aerospike cluster to work through these examples — see [`source/java`'s Setup section](source/java/README.md#setup) if you don't have one already; no further instruction on setting up Aerospike clusters is provided here.

Several of the use cases below rely on ACID transactions, which Aerospike introduced in version 8 and which require the `test` namespace to be configured with `strong-consistency` (an enterprise feature). To let these demonstrations run on any cluster, each implementation detects at startup whether the cluster/namespace meets that bar; if not, it warns and silently disables the transactional aspects for that run rather than failing outright.

# Use Cases
There are multiple use cases in this repository, each with a detailed explanation and sample code. Each implementation linked above provides its own way to run and explore them (an interactive menu for `source/java`, a `-uc <name>` CLI flag for `source/java-sdk`) — see that implementation's README for specifics.

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

## Versioning Records
Maintain historical versions of records with point-in-time query capabilities. Demonstrates atomic version creation using transactions and time-based queries using map operations. Objects are assumed to have 2 parts -- a base record which changes frequently and is small, and a details record which is large and changes infrequently.  See [Versioning Records](UseCases/versioning-records.md)

## Delta Versioning Records
Maintain an audit trail of record changes using delta records instead of full copies. Detects bin-level changes server-side with expressions, stores who changed what and when, and optionally stores new bin values for record reconstruction. See [Delta Versioning Records](UseCases/versioning-records-delta.md)

## Hot Key — Read (Replica Spread)
Spread read load across identical replica records. Runs baseline (single key) and mitigation (random replica) phases with per-second KEY_BUSY and latency metrics. See [Hot Key — Read (Replica Spread)](UseCases/hot-key-read-replica-spread.md)

## Hot Key — Write (Shard + Merge)
Shard write load across multiple keys and merge totals with batch reads. See [Hot Key — Write (Shard + Merge)](UseCases/hot-key-write-shard-merge.md)

## Hot Key — Write (HotKeyReducer)
Batch concurrent operates in-process before sending to Aerospike. See [Hot Key — Write (HotKeyReducer)](UseCases/hot-key-write-reducer.md)

# Contributing

## Adding a New Use Case
Want to contribute your own use case to this cookbook? We'd love to have your contribution! See our comprehensive guide on [How to Add a New Use Case](CONTRIBUTING.md) (currently written against `source/java`, the legacy client implementation) which includes:

- Step-by-step instructions for creating a use case
- Complete, working example with ~15 fields and @GenMagic
- How to use the Async library (`runFor`, `periodic`, `continuous`)
- Using Parameters instead of hard-coded constants
- Data model best practices
- Documentation guidelines
- AQL query examples
- End-to-end testing procedures

The guide walks you through a fully worked example of an Inventory Management System that demonstrates all the patterns and best practices used in this repository.
