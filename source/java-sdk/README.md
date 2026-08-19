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

**Prefer `TypedDataSet<T>`/`TypedKey<T>` over raw `DataSet`/`Key`** wherever a use case actually
does object mapping (i.e. calls `.object(pojo)` to write, or wants a decoded `T` back on read) —
every use case in this port follows this now:

- Declare the dataset as `TypedDataSet.of(namespace, set, Foo.class)` instead of `DataSet.of(...)`.
- `.id(...)` on it returns a `TypedKey<Foo>` instead of a plain `Key`; `session`/transaction
  `upsert`/`query`/`delete`/etc. all have matching `TypedKey<T>`/`TypedKeyList<T>` overloads.
- **Writes:** unchanged in shape — `session.upsert(typedDataSet).object(pojo).execute()` — but now
  type-checked against `T` instead of accepting any `Object`.
- **Reads:** `session.query(typedKey).execute()` returns a `TypedRecordStream<T>` with
  `getFirstObject()`/`toObjectList()`/`forEachObject(Consumer<T>)`, which decode via the registered
  mapper automatically. This removes the manual
  `session.getCluster().getRecordMappingFactory().getMapper(Foo.class).fromMap(record.bins, key, generation)`
  step entirely — no use case in this port needs to call `getMapper(...)` directly anymore.

Raw `DataSet`/`Key` are still the right tool where there's no single mapped type to decode to —
e.g. `TimeSeriesDemo`/`TimeSeriesLargeVarianceDemo`'s event records (events live nested inside a
bucket record's map bin, not as their own top-level object) and the hot-key use cases' replica
records (raw bin writes to a custom `productId:index` key, not the model's own mapped id).

The [Java Object Generator](https://github.com/aerospike-examples/java-object-generator) isn't
wired up here (same manual-install gap as `../java`) — sample data is hand-generated with
`ThreadLocalRandom` instead.

## Expressions: prefer AEL over nested `Exp` builder chains

For simple filter/read expressions, the `Exp`/`MapExp`/`ListExp` fluent builders read fine. Once an
expression nests more than one or two levels deep (`Exp.let` + several `Exp.def`/`Exp.cond`
levels), it gets hard to read and easy to mis-nest. This SDK can compile **AEL** (Aerospike
Expression Language) strings directly wherever an `Exp`/`Expression` is accepted —
`BinBuilder.selectFrom(String)`/`upsertFrom(String)` parse the string themselves (no separate
`AelMaterializer` call needed for that path); `AelMaterializer.expressionFromString(Cluster,
String)` is available if you need a reusable `Expression` object instead (e.g. for `where(...)`).

`AdvancedExpressions.multipleCommandsInOneOperation` is the reference example — the legacy client's
4-level `Exp.let`/`Exp.def`/`Exp.cond` chain becomes:

```
let (
  color = when ($.color == 'Purple' => $.features.append('Great Color'), default => $.features),
  type  = when ($.bodyType == 'CONVERTIBLE' => (${color}).append('Looks Cool'), default => ${color}),
  power = when ($.engineSize > 5.0 => (${type}).append('Powerful'), default => ${type}),
  age   = when ($.year >= 2020 => (${power}).append('New-ish'), default => ${power})
) then (${age})
```
passed straight to `.bin("features").upsertFrom("""...""")` — verified against a live cluster to
produce identical results to the builder-chain version.

**This doesn't extend to every CDT composition yet.** `Leaderboard.getScoresAroundPlayer` composes
a map `getByKey(..., INDEX)` lookup with a `getByIndexRange`; the equivalent AEL dot-call syntax
(`$.score.getByKey('key', INDEX)`) parses fine client-side but returns a server-side `Parameter
error` at execution time — almost certainly because an `INDEX`-return map lookup needs an explicit
value-type hint (the Java API requires one, `Exp.Type.INT`, as an explicit argument) that isn't
documented anywhere accessible from this build. Rather than guess at syntax for a
scoring-correctness-critical expression, that one stays on the already-verified `Exp` builder form
- see the comment on `getScoresAroundPlayer` for details. Worth revisiting once an authoritative
AEL grammar reference for map/list return-type composition is available.

## Known limitations (alpha SDK)

- **No secondary transaction/query API for the interactive menu, search, or CLI parameter
  overrides yet** — see Setup above.
- **This cluster's `test` namespace needs `strong-consistency` for real multi-record
  transactions**, same as `../java`. If it isn't configured, `UseCaseCookbookRunner` detects this
  at startup and swaps in `NonTransactionalCapableSession`, which runs `doInTransaction`/
  `doInTransactionReturning` without a real `Txn` attached — same idea as the legacy client's
  `AerospikeClientProxy` shim, just implemented via the SDK's `SessionExtension` hook instead of a
  dynamic proxy.
- **A few nested/composed CDT expressions don't evaluate correctly against this cluster** (nested
  `MapExp` compositions returning `Parameter error`, and some write-side conditional expressions
  chaining multiple CDT ops). Where hit, the affected use case (`TimeSeriesDemo`'s device filter,
  `TimeSeriesLargeVarianceDemo`'s bucket-split write path, `TopTransactionsAcrossDcs`'s DC-map
  merge) falls back to an equivalent client-side implementation instead of forcing the
  server-side version — each is called out in a comment at the top of the affected class. Note:
  the specific `TimeSeriesDemo` device-filter gap was confirmed to reproduce identically on the
  mature legacy Java client (`../java`) against the same cluster build, so that one is a
  server-side expression-evaluator limitation on this Aerospike build, not an SDK-maturity gap —
  worth re-checking against a different server build rather than a different SDK version.
- Not every alpha SDK method exists on both the read and write sides of the fluent API — e.g.
  `onMapKeyRelativeIndexRange` is only on the write-side `BinBuilder`, not the read-side
  `QueryBinBuilder`. `VersioningRecords` works around this by issuing a read-only lookup through
  `session.upsert(...)` (no bin is actually set, so nothing is mutated).

None of the above are code bugs to "fix" in this port — they're the actual current behavior of the
alpha SDK build this was written against (`0.9.0-alpha.2`). If a future SDK release closes these
gaps, the client-side fallbacks can be swapped back for the more direct server-side approach.
