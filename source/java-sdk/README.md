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
passed straight to `.bin("features").upsertFrom("""...""")` — verified against a live cluster to
produce identical results to the builder-chain version.

**Use the canonical AEL reference, not third-party grammar repos.** The full language reference is
saved at [`AEL_CANONICAL_REFERENCE.md`](../AEL_CANONICAL_REFERENCE.md) (courtesy of Tim Faulkes) —
treat it as ground truth over any other grammar source when writing or debugging AEL. An earlier
pass at this port used a different, non-canonical third-party grammar repo and got compositions
wrong as a result — all fixed once checked against the canonical doc:
- `AdvancedExpressions`'s single-element list read now uses `$.acc.[0]:INT` — the type suffix was
  missing; the earlier attempts guessed at nonexistent terminal methods instead
  (`.get(return: INDEX)`) and failed for that reason, not because the read itself was unsupported.
- `Leaderboard.getScoresAroundPlayer`'s combined index-lookup-plus-windowed-range read — previously
  assumed impossible in AEL entirely — collapses to a single relative-range map selector,
  `{-N:N~'key'}` (§5 of the canonical reference), replacing the original 5-line nested
  `Exp.let`/`Exp.def`/`Exp.cond` + two separate `MapExp` reads. Verified against a live cluster with
  a full (not sampled) diff against the original `Exp` composition across many buckets —
  byte-identical, including automatic boundary clamping at the map's edges.
- `TimeSeriesDemo`'s device filter (and `TimeSeriesLargeVarianceDemo`'s identical read path) —
  previously believed to need an unsupported nested-`MapExp` wildcard-value-list match, and left as
  a client-side fallback — collapses to a map key-range selector chained with a filter against the
  canonical reference's §4.4 `&[?(…)]` form (not `.*[?(…)]` wildcard iteration, which doesn't
  combine with a preceding range selector the same way):
  `$.map.{@'<earliest>':'<latest>'}&[?(@.[0] in ['dev1','dev2'])]`. Verified live across a
  multi-device, multi-page query — matches exactly the requested device IDs and nothing else. §12's
  restriction that `getMaps()` doesn't work after a filter turned out not to matter: implicit-get on
  a map range/filter path already returns a flat, key-ordered LIST of values, which is all this use
  case ever needed (it never used the map keys, only iteration order).
- `TimeSeriesLargeVarianceDemo`'s bucket-split write path — previously believed to need an
  unsupported nested conditional write, and left as a plain read-then-write fallback — works as one
  `operate()` call against the bucket record: a `when($.map:MAP.count() >= N => …, default => …)`
  guard, `getMaps()` for the conditional minority read (needs a real selector like `{0:}`, not the
  bare pinned bin - `getMaps()` on `$.map:MAP` alone is a parse error per §12), and `.remove()` as
  the conditional majority write. One real constraint this surfaced: selector bounds (`{…}`) must be
  static literals (§4.2) - `{:count()*80/100}` doesn't compile - so the split point had to become a
  fixed item count (matching how the legacy client actually derives its own split constant) rather
  than a live percentage of the current bucket size. Verified live at both ends: an overflowing
  bucket (12 items, threshold 10) correctly removes and returns its last 2; a non-overflowing one
  (5 items) is left untouched and returns an empty minority - then load-tested via the full 25,000+
  event `acct-1` setup run, which splits buckets thousands of times.
- `TopTransactionsAcrossDcs`'s DC-map merge — previously believed to need an unsupported nested
  conditional expression, and left as a client-side `TreeMap` merge — works as one read:
  `putItems` merges one DC map into the other (`$.dc1.putItems($.dc2)`), but the merged result
  can't be selector-ranged directly (`putItems()` is a path write terminal, so the path ends there
  - `$.dc1.putItems($.dc2).{-N:}` is a parse error). Binding the merge to a `let` variable and
  re-navigating from `(${var})` - the canonical reference's rule (§4.2) for continuing a path after
  a parenthesised expression - makes the range selector work on the merged value:
  `let (merged = $.dc1.putItems($.dc2)) then ((${merged}).{-N:})`. Wrapped in a `when` to handle a
  DC bin not existing yet (bin navigation is strict by default, so referencing a missing bin
  throws) - common for any account without transactions in one or both DCs early in a run. Verified
  live for all three presence combinations (both bins, one bin, neither bin) and via the full
  25-second simulation run, which showed correctly time-descending, correctly-interleaved results
  from both DCs at every display tick, including the very first one (zero transactions yet).

That doesn't mean every "AEL can't do this" conclusion in this port turns out wrong once you check
the canonical reference — `DeltaVersioningRecords`'s `versions`-map pointer update (find the map
entry currently marked -1, close it, open a new one) was tried directly against the canonical
reference too, and failed for a real, documented reason: selector operands and collection-literal
keys must be static literals, never a bin path, `$`-expression, or `let` variable (§4.2, §5) — so a
key discovered at runtime via a value selector can't then be used to address a write. See that
class's javadoc for the exact attempts and error output.

Moral: an AEL expression that "can't" be written is usually a syntax gap on the author's side, not a
language gap — check the canonical reference before concluding otherwise. But "check the canonical
reference" means actually trying it and reading the error, not just re-reading the doc and guessing
again — some things really are out of scope, and the difference is empirical, not textual.

## Known limitations (alpha SDK)

- **No CLI parameter overrides yet** (`--param.<name>=<value>`) — see Setup above. The
  interactive menu and search are implemented.
- **This cluster's `test` namespace needs `strong-consistency` for real multi-record
  transactions**, same as `../java`. If it isn't configured, `UseCaseCookbookRunner` detects this
  at startup and swaps in `NonTransactionalCapableSession`, which runs `doInTransaction`/
  `doInTransactionReturning` without a real `Txn` attached — same idea as the legacy client's
  `AerospikeClientProxy` shim, just implemented via the SDK's `SessionExtension` hook instead of a
  dynamic proxy.
- **A few nested/composed CDT expressions may not evaluate correctly against this cluster** (nested
  `MapExp` compositions returning `Parameter error`, and some write-side conditional expressions
  chaining multiple CDT ops) — but see the note below: every use case that previously fell back to
  a client-side implementation for this reason (`TimeSeriesDemo`'s device filter,
  `TimeSeriesLargeVarianceDemo`'s bucket-split write path, `TopTransactionsAcrossDcs`'s DC-map
  merge) has since been re-derived directly in AEL (not the `Exp`/`MapExp` builder API) using the
  canonical reference and confirmed to work fine, live, under load. None of this port's use cases
  currently have an open AEL gap; this bullet stays as a general caution for future AEL work, not
  a list of known-broken spots.
- Not every alpha SDK method exists on both the read and write sides of the fluent API — e.g.
  `onMapKeyRelativeIndexRange` is only on the write-side `BinBuilder`, not the read-side
  `QueryBinBuilder`. `VersioningRecords` works around this by issuing a read-only lookup through
  `session.upsert(...)` (no bin is actually set, so nothing is mutated).

None of the above are code bugs to "fix" in this port — they're the actual current behavior of the
alpha SDK build this was written against (`0.9.0-alpha.2`). If a future SDK release closes these
gaps, the client-side fallbacks can be swapped back for the more direct server-side approach.
