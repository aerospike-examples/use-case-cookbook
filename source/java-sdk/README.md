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

CLI options:
- `-uc, --useCaseName <name>` — run a use case (partial name match allowed); if omitted, launches
  the interactive menu instead
- `-l, --listUseCases` — list all registered use cases
- `-ro, --runOnly` / `-so, --seedOnly` — skip `setup()` or `run()` respectively

The interactive menu (`InteractiveMenu`) matches `../java`'s: `search <term>`/`s <term>` (text),
`/<regex>` (regex), `clear`/`c`, `summary`/`full`, and numbered parameter editing for any use case
that declares `Parameter<T>`s (see `../java/README_SEARCH.md` for the command reference, unchanged
here). Not yet ported: `--param.<name>=<value>` CLI overrides (`../java`'s `ParameterParser`/
`BatchExecutor` equivalent).

## Object mapping

The legacy client examples use the standalone
[Java Object Mapper](https://github.com/aerospike/java-object-mapper). This module uses
[`aerospike-sdk-mapper-java`](https://github.com/aerospike/aerospike-sdk-mapper-java) instead:
annotate each model with `@AerospikeRecord(namespace=, set=)`/`@AerospikeKey`, then register one
`AeroMapper` for the whole run — `AeroMapper.Builder(session).build()` followed by
`cluster.setRecordMappingFactory(aeroMapper.asMappingFactory())` in `UseCaseCookbookRunner`, before
the real per-use-case `Session` is created (see that class's javadoc for why a bootstrap `Session`
is needed first). Every model that has a single mapped Aerospike record type uses this — there are
no hand-written `RecordMapper<T>` implementations left in this module.

Annotation attributes must be compile-time constants, so every model hardcodes
`namespace = "test"` rather than reading the `-Ddemo.namespace` JVM system property this repo
otherwise supports. This only matters if `demo.namespace` is overridden away from `"test"`, which
no use case here does.

One confirmed library limitation: `AeroRecordMapper`'s 3-arg `fromMap(Map, Key, int)` — the
overload the SDK's untyped `RecordStream.getFirst(RecordMapper<T>)`/`.pop(...)` call — always
throws `UnsupportedOperationException`, since the mapper only decodes through the SDK's typed
query path (which supplies the `RecordReadContext` it needs). This affects any read that comes
back from an upsert-with-conditional-filter-and-read-back rather than a query; `PlayerMatching`'s
`findPlayerToAttack`/`setPlayerOnline` hit it and decode the `Record` manually instead (see
`PlayerMatching.recordToPlayer`).

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

## Expressions: AEL, not `Exp`/`MapExp`/`ListExp` builder chains

Every expression in this module is written as an **AEL** (Aerospike Expression Language) string,
not the `Exp`/`MapExp`/`ListExp` fluent builder API — nothing in this module builds an `Exp` tree.
`BinBuilder.selectFrom(String)`/`upsertFrom(String)` (and the other write-side `*From(String)`
methods) parse an AEL string directly wherever an `Exp`/`Expression` is accepted; `AelMaterializer
.expressionFromString(Cluster, String)` is available if a reusable `Expression` object is needed
instead (e.g. for `where(...)`).

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
passed straight to `.bin("features").upsertFrom("""...""")`.

**Use [`AEL_CANONICAL_REFERENCE.md`](AEL_CANONICAL_REFERENCE.md), not third-party grammar
repos**, when writing or debugging AEL in this module. Patterns used here that aren't obvious from
a first read of the grammar:
- A single-element scalar read needs an explicit `:TYPE` suffix: `$.acc.[0]:INT`
  (`AdvancedExpressions`).
- A relative-range map selector, `{-N:N~'key'}` (§5), reads N entries either side of a known key in
  one call (`Leaderboard.getScoresAroundPlayer`).
- A map key-range selector chained with a filter, `{@'a':'b'}&[?(@.[0] in ['x','y'])]` (§4.4) — not
  `.*[?(…)]` wildcard iteration, which doesn't combine with a preceding range selector — filters
  within a range server-side (`TimeSeriesDemo`/`TimeSeriesLargeVarianceDemo` device filter).
- Selector bounds (`{…}`) must be static literals (§4.2) — a computed bound like `count()*80/100`
  doesn't compile. `TimeSeriesLargeVarianceDemo`'s bucket split uses a fixed item count for this
  reason. `getMaps()` also needs a real selector, not a bare pinned bin — `$.map:MAP` alone is a
  parse error (§12).
- `putItems()`/`remove()`/other path write terminals end the path — to select from a computed
  value, bind it with `let (var = <expr>) then ((${var}).<further path>)`
  (`TopTransactionsAcrossDcs`'s DC-map merge).
- Selector operands and collection-literal keys must also be static literals — a key discovered at
  runtime via a value selector can't then address a write. This is why `DeltaVersioningRecords`
  still diffs client-side; see that class's javadoc.

## Known limitations (alpha SDK)

- **No CLI parameter overrides yet** (`--param.<name>=<value>`) — see Setup above. The
  interactive menu and search are implemented.
- **This cluster's `test` namespace needs `strong-consistency` for real multi-record
  transactions**, same as `../java`. If it isn't configured, `UseCaseCookbookRunner` detects this
  at startup and swaps in `NonTransactionalCapableSession`, which runs `doInTransaction`/
  `doInTransactionReturning` without a real `Txn` attached — same idea as the legacy client's
  `AerospikeClientProxy` shim, just implemented via the SDK's `SessionExtension` hook instead of a
  dynamic proxy.
- Not every alpha SDK method exists on both the read and write sides of the fluent API — e.g.
  `onMapKeyRelativeIndexRange` is only on the write-side `BinBuilder`, not the read-side
  `QueryBinBuilder`. `VersioningRecords` works around this by issuing a read-only lookup through
  `session.upsert(...)` (no bin is actually set, so nothing is mutated).

None of the above are code bugs to "fix" in this port — they're the actual current behavior of the
alpha SDK build this was written against (`0.9.0-alpha.2`).
