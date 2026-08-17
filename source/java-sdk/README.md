# Use Case Cookbook — Java SDK

This is a port of the [Use Case Cookbook](../../README.md)'s use cases onto Aerospike's new Java
SDK (`com.aerospike:aerospike-client-sdk`), as opposed to the legacy Java client examples in
[`../java`](../java/README.md). The new SDK is currently **alpha** — expect some rough edges,
called out below.

## Setup

You need a running Aerospike cluster (see [`../java`](../java/README.md#setup) for pointers if you
don't have one) and **JDK 21** (the SDK requires it; the legacy `../java` module only needs
Java 11).

```
cd source/java-sdk
mvn clean package
java -jar target/use-case-cookbook-sdk-0.1.0-SNAPSHOT-full.jar -uc "Demo setup"
```

If no seed node is passed, `localhost:3000` is assumed — use `-h <host:port>` to connect elsewhere,
and `--help` for the rest of the connection options. The namespace defaults to `test`; override
with `-Ddemo.namespace=<name>` as a JVM system property.

Only the non-interactive, named-use-case path is implemented so far:
- `-uc, --useCaseName <name>` — run a use case (partial name match allowed)
- `-l, --listUseCases` — list all registered use cases
- `-ro, --runOnly` / `-so, --seedOnly` — skip `setup()` or `run()` respectively

There's no interactive menu, search, or `--param.<name>=<value>` override yet (all present in
`../java`) — this module is intentionally a work in progress.

## Object mapping

The legacy client examples use the standalone
[Java Object Mapper](https://github.com/aerospike/java-object-mapper). This SDK has its own
built-in mapping mechanism instead: implement `RecordMapper<T>` per model class, then register
every mapper once in `UseCaseCookbookRunner.buildMappers()` via
`Cluster.setRecordMappingFactory(new DefaultRecordMappingFactory(mappers))` — mirroring the single
client+mapper pair the legacy runner builds and hands to every use case.

- **Writes:** `session.upsert(dataSet).object(pojo).execute()` (also `insert`/`update`/`replace`).
- **Reads:** there's no `RecordResult.as(Class)` shortcut in this SDK version, so fetch the
  registered mapper and decode manually:
  `session.getCluster().getRecordMappingFactory().getMapper(Foo.class).fromMap(record.bins, result.getKey(), record.generation)`.

The [Java Object Generator](https://github.com/aerospike-examples/java-object-generator) isn't
wired up here (same manual-install gap as `../java`) — sample data is hand-generated with
`ThreadLocalRandom` instead.

## Known limitations (alpha SDK)

- **No secondary transaction/query API for the interactive menu, search, or CLI parameter
  overrides yet** — see Setup above.
- **This cluster's `test` namespace needs `strong-consistency` for real multi-record
  transactions**, same as `../java`. If it isn't configured, `UseCaseCookbookRunner` detects this
  at startup and swaps in `NonTransactionalCapableSession`, which runs `doInTransaction`/
  `doInTransactionReturning` without a real `Txn` attached — same idea as the legacy client's
  `AerospikeClientProxy` shim, just implemented via the SDK's `SessionExtension` hook instead of a
  dynamic proxy.
- **A few nested/composed CDT expressions don't evaluate correctly on this alpha build** (nested
  `MapExp` compositions returning `Parameter error`, and some write-side conditional expressions
  chaining multiple CDT ops). Where hit, the affected use case (`TimeSeriesDemo`'s device filter,
  `TimeSeriesLargeVarianceDemo`'s bucket-split write path, `TopTransactionsAcrossDcs`'s DC-map
  merge) falls back to an equivalent client-side implementation instead of forcing the
  server-side version — each is called out in a comment at the top of the affected class. Worth
  re-checking against later SDK builds.
- Not every alpha SDK method exists on both the read and write sides of the fluent API — e.g.
  `onMapKeyRelativeIndexRange` is only on the write-side `BinBuilder`, not the read-side
  `QueryBinBuilder`. `VersioningRecords` works around this by issuing a read-only lookup through
  `session.upsert(...)` (no bin is actually set, so nothing is mutated).

None of the above are code bugs to "fix" in this port — they're the actual current behavior of the
alpha SDK build this was written against (`0.9.0-alpha.2`). If a future SDK release closes these
gaps, the client-side fallbacks can be swapped back for the more direct server-side approach.
