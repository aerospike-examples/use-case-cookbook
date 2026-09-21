# Use Case Cookbook — Python SDK

This is a port of the [Use Case Cookbook](../../README.md)'s use cases onto Aerospike's new Python
SDK ([`aerospike-sdk`](https://pypi.org/project/aerospike-sdk/) on PyPI, package `aerospike_sdk`),
as opposed to the legacy Java client examples in [`../java`](../java/README.md) or the Java SDK
examples in [`../java-sdk`](../java-sdk/README.md). The new SDK is currently **public preview
(alpha)** — expect some rough edges, called out below.

## Setup

You need a running Aerospike cluster (see [`../java`](../java/README.md#setup) for pointers if you
don't have one) and **Python 3.10–3.14**.

```
cd source/python-sdk
python3.12 -m venv .venv        # or any 3.10-3.14 interpreter
source .venv/bin/activate
pip install -r requirements.txt
python main.py -uc "Demo setup"
```

If no seed host is passed, `localhost:3000` is assumed — use `--hosts <host:port>` to connect
elsewhere, and `--help` for the rest of the connection options (`-U`/`-P` for INTERNAL-mode auth,
`-cn` for cluster name, `-sa` for services-alternate). The namespace defaults to `test`; override
with the `DEMO_NAMESPACE` environment variable (this module's equivalent of the other two modules'
`-Ddemo.namespace` JVM system property — Python has no per-process system-property mechanism).

CLI options:
- `-uc, --useCaseName <name>` — run a use case (partial name match allowed); if omitted, launches
  the interactive menu instead
- `-l, --listUseCases` — list all registered use cases
- `-ro, --runOnly` / `-so, --seedOnly` — skip `setup()` or `run()` respectively
- `--param.<name>=<value>` — override a use case's `Parameter` from the command line

The interactive menu (`InteractiveMenu`) matches `../java`/`../java-sdk`'s: `search <term>`/
`s <term>` (text), `/<regex>` (regex), `clear`/`c`, `summary`/`full`, and numbered parameter editing
for any use case that declares `Parameter`s (see `../java/README_SEARCH.md` for the command
reference, unchanged here).

## Object mapping

This SDK has **no annotation-driven object mapper at all** — unlike `aerospike-sdk-mapper-java` on
the Java SDK side, there's nothing to register or wire up. Every model in this port is a plain
`@dataclass` with hand-written `to_bins()`/`from_bins(bins)` methods (see
[`usecasecookbook/setup/model.py`](usecasecookbook/setup/model.py) for the pattern every use case
follows): `to_bins()` returns the `dict` handed to `session.upsert(key).put(...)`, and
`from_bins(record.bins)` reconstructs the dataclass on read.

Where a use case has no single mapped type to decode to — event records nested inside a bucket's
map bin (`timeseries`), or hot-key replica records under a raw `productId:index` key (`hotkeys`) —
bins are read/written directly as `dict`s, same as the other two modules' equivalent raw-bin cases.

There's no equivalent of the Java Object Generator wired up here either (same manual-install gap as
`../java`) — sample data is hand-generated with the standard library `random` module instead.

## Expressions: AEL — and a real gotcha in how this port verified it

This SDK supports **AEL** (Aerospike Expression Language) via `.where(ael_string)` (query filters)
and `.bin(x).select_from(ael_string)` (computed reads) — same idea as `../java-sdk`'s AEL support.
The canonical AEL grammar is the same language across every Aerospike client, and its type-suffix
convention (`$.bin:INT`) and write-shaped path terminals (`.append(value)`, `.putItems(...)`, etc.)
are standard, not Java-SDK-specific — `../java-sdk` uses them because it implements them correctly,
not because it's a different language from what this module targets.

**Real, confirmed issue (found in code review, not caught earlier in this port): the currently
published `aerospike-sdk==0.9.0a5` package does not implement that canonical grammar.** This
package parses AEL strings with its own bundled, client-side grammar
(`aerospike_sdk/ael/antlr4/Condition.g4`) rather than sending them to the server for compilation.
That bundled grammar is frozen at a June 2026 snapshot and predates the canonical `:TYPE` suffix
and write-shaped path terminals entirely — confirmed empirically: every `:TYPE`-suffixed
expression this port tried against a live cluster fails identically
(`AelParseException: mismatched input ':' expecting <EOF>`), regardless of which construct it's
attached to. The SDK's actively developed branch has since moved AEL compilation **server-side**
(requiring Aerospike **8.2.0+** — this repo's dev cluster is 8.1.3.0, below that bar) and adopted
the canonical grammar, but that branch depends on an internal, not-publicly-published package, so
it can't currently be installed or verified against by anyone following this repo's own
`pip install -r requirements.txt` instructions.

**Practical effect on this port**: every AEL string here uses this package's older, non-canonical
dialect (`.get(type: INT)` instead of `:INT`, no write-shaped terminals, no `&[?(...)]` filter
chains) because that's the only dialect the publicly-installable package can parse — not because
the underlying language lacks these features. Each affected call site has a code comment
explaining this precisely, with the concrete parser error, rather than the earlier (incorrect)
framing that these were permanent design differences in "this SDK's own AEL dialect." Notably,
the reason this port has correctness-preserving workarounds everywhere it needed a write-shaped
AEL operation (`AdvancedExpressions`'s conditional feature-augmentation, `TopTransactionsAcrossDcs`'s
cross-DC map merge) is real and still applies — those workarounds aren't wrong, only their stated
justification for existing was.

**This is a gap in the AEL *string* grammar specifically, not in expression-based writes overall**:
`select_from`/`insert_from`/`update_from`/`upsert_from` all accept `Union[str, FilterExpression]`,
and `aerospike_sdk.Exp` (`FilterExpression` from the underlying async client) is a full
`Exp`/`MapExp`/`ListExp`-equivalent builder (`cond`, `def_`/`var`/`exp_let`, `bin_exists`,
`bin_type`, `map_get_by_value`, `map_put`, `unknown`, etc.) — nearly 200 methods, comparable in
breadth to the Java SDK's `Exp` API, and unaffected by the stale-AEL-grammar issue above since it
builds an expression tree directly rather than parsing text. `recordversioning/delta_versioning_records.py`
uses this to do the same server-side snapshot-and-compare technique the Java SDK port does, in one
round trip, rather than a naive client-side read/write/read diff — see its docstring.

Remaining gotchas confirmed to be genuine canonical-grammar constraints (not stale-package
artifacts) rather than SDK-specific ones:
- Selector operands (`{...}`/`[...]`) must be static literals, not computed expressions — canonical
  AEL itself, not just this package (explicitly documented as such, and marked "will change in a
  later release").
- A bare `$.bin.count()` on a CDT bin raises `OpNotApplicable` — it needs the map-type-designator
  token: `$.bin.{}.count()`.

**CDT builder API is otherwise close to 1:1 with the Java SDK's** — `on_map_key`/`on_map_index`/
`on_map_key_range`/`on_map_value_range`/`on_map_key_relative_index_range`/`on_list_index`/etc. exist
on **both** read and write builders in this SDK (no read/write asymmetry gap for
`on_map_key_relative_index_range` the way the Java SDK's alpha build had).

## Known limitations (alpha SDK)

- **This cluster's `test` namespace needs `strong-consistency` for real multi-record
  transactions**, same as `../java`/`../java-sdk`. Detection is much simpler here than on the Java
  SDK side: `SyncSession.is_namespace_sc(namespace)` reports this directly (no throwaway-write
  probe needed). `usecasecookbook/txn.py`'s `run_in_transaction(session, fn)` calls
  `session.do_in_transaction(fn)` when SC is on, or just `fn(session)` directly (no real transaction
  object at all) when it's off — since `SyncTransactionalSession` is a superset of `SyncSession` for
  every method a use case actually calls, no subclassing/proxying is needed the way the Java SDK's
  `NonTransactionalCapableSession` shim requires.
- `aerospike_async.Key` objects are **not hashable** in this SDK, unlike the Java client's `Key` —
  code that needs a per-key lookup structure (e.g. `hotkeys/reducer.py`'s batching map) keys on
  `key.digest` (a hashable `str`) instead of the `Key` object itself.
- No per-operation results array equivalent to the Java SDK's `Record.results[]` was found for a
  same-bin multi-`.add()` write with no read-back requested — such a write returns an empty `bins`
  dict. `hotkeys/reducer.py` doesn't need per-operation unpacking either way (callers only care about
  success/failure), so this wasn't a blocker, just a simplification opportunity the Java SDK had that
  this port didn't need.

None of the above are code bugs to "fix" in this port — they're the actual current behavior of the
alpha SDK build this was written against (`aerospike-sdk==0.9.0a5`).
