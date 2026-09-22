# Use Case Cookbook — Python SDK

This is a port of the [Use Case Cookbook](../../README.md)'s use cases onto Aerospike's new Python
SDK ([`aerospike-sdk`](https://pypi.org/project/aerospike-sdk/) on PyPI, package `aerospike_sdk`),
as opposed to the legacy Java client examples in [`../java`](../java/README.md) or the Java SDK
examples in [`../java-sdk`](../java-sdk/README.md). The new SDK is currently **public preview
(alpha)** — expect some rough edges, called out below.

## Setup

You need **Python 3.12+** (the version this was built and tested against) and a running Aerospike
cluster on **build 8.2.0 or later** (see [`../java`](../java/README.md#setup) for general
cluster-setup pointers) — this module's AEL usage requires server-side AEL compilation, only
available from 8.2.0 onward (see "Expressions: AEL" below).

**This module currently pins a pre-release SDK build not yet on public PyPI** - see
`requirements.txt`. This isn't just an AEL-syntax difference: the public PyPI release predates the
`aerospike_sdk.sync.Session`/`TransactionalSession` names this port imports throughout, so nothing
here runs against it. This will switch back to a plain public-PyPI pin once a public release with
that API shape ships.

```
cd source/python-sdk
python3.12 -m venv .venv
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

## Expressions: AEL

This SDK supports **AEL** (Aerospike Expression Language) via `.where(ael_string)` (query filters),
`.bin(x).select_from(ael_string)` (computed reads), and `.bin(x).upsert_from(ael_string)`/
`insert_from`/`update_from` (computed writes) — the same canonical grammar `../java-sdk` uses,
including type-suffix path pins (`$.bin:INT`), write-shaped path terminals (`.append(value)`,
`.putItems(...)`, etc.), and wildcard/key-range filter chains (`&[?(...)]`). AEL strings are
compiled **server-side**, which is why this needs Aerospike 8.2.0+ and the pre-release SDK build
pinned in `requirements.txt` (see Setup above).

`recordversioning/delta_versioning_records.py` is the one place that uses the programmatic
`aerospike_sdk.Exp` builder instead of an AEL string, for the same reason
`../java-sdk` does: its snapshot-and-compare technique needs to close a map entry discovered at
runtime by value, and AEL selector operands must be static literals. `select_from`/`insert_from`/
`update_from`/`upsert_from` all accept `Union[str, FilterExpression]`, so AEL and `Exp` mix freely
within one call.

**Dataset-level `.where()` queries require a secondary index** (or an explicit opt-in) on clusters
with query selection, which 8.2.0 has — a `.where()` query the server can't satisfy with an index
is rejected rather than falling back to a full-set scan. Use cases here that intentionally do a
full scan opt in with `.with_hint(QueryHint(allow_scans_with_where=True))` (see
`advancedexpressions/advanced_expressions.py`). Single-key and batch (explicit key list) queries
are unaffected.

**CDT builder API is close to 1:1 with the Java SDK's** — `on_map_key`/`on_map_index`/
`on_map_key_range`/`on_map_value_range`/`on_map_key_relative_index_range`/`on_list_index`/etc. exist
on both read and write builders here.

Selector operands (`{...}`/`[...]`) must be static literals, not computed expressions — a
canonical-grammar constraint, not an SDK-specific gap.
`timeseries/time_series_large_variance_demo.py`'s bucket-split point is a fixed item count rather
than a computed percentage for this reason.

## Known limitations (alpha SDK)

- **This cluster's `test` namespace needs `strong-consistency` for real multi-record
  transactions**, same as `../java`/`../java-sdk`. `Session.is_namespace_sc(namespace)` reports
  this directly. `usecasecookbook/txn.py`'s `run_in_transaction(session, fn)` calls
  `session.do_in_transaction(fn)` when SC is on, or just `fn(session)` directly when it's off —
  `TransactionalSession` is a superset of `Session` for every method a use case calls, so no
  subclassing/proxying is needed the way the Java SDK's `NonTransactionalCapableSession` requires.
- `aerospike_async.Key` objects are **not hashable** in this SDK, unlike the Java client's `Key` —
  code that needs a per-key lookup structure (e.g. `hotkeys/reducer.py`'s batching map) keys on
  `key.digest` (a hashable `str`) instead.
- No per-operation results array equivalent to the Java SDK's `Record.results[]` — a same-bin
  multi-`.add()` write with no read-back returns an empty `bins` dict. `hotkeys/reducer.py` doesn't
  need per-operation unpacking either way.
- `session.info().namespace_details(namespace)` returns a structured `NamespaceDetail` with a fixed
  field set that doesn't include arbitrary config like `transaction-pending-limit`.
  `hotkeys/pending_limit_scope.py` uses the generic `session.info().info(command)` (raw
  `get-config`/`set-config` text) for that instead.

None of the above are code bugs to "fix" in this port — they're the actual current behavior of the
SDK build this was written against (`aerospike-sdk==0.9.0a6.dev95`).
