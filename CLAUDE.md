# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A collection of runnable Java examples ("use cases") showing how to model and solve hard problems with Aerospike (one-to-many/many-to-many relationships, leaderboards, player matching, time series, hot keys, record versioning, cross-DC transaction merging, advanced expressions, etc.). Each use case has a paired markdown doc in `UseCases/` explaining the design decisions, data model, and AQL examples. Design patterns are language-agnostic; the code itself is Java.

All Java source lives under `source/` (a self-contained Maven module) — `cd source` before running any Maven command.

## Build & run

```bash
cd source
mvn clean package                 # builds target/use-case-cookbook-<version>-full.jar (shaded, with all deps)
java -jar target/use-case-cookbook-<version>-full.jar                      # interactive menu, connects to localhost:3000
java -jar target/use-case-cookbook-<version>-full.jar -h <host:port>       # connect to a specific seed node
```

There is no test suite in this repo (`junit-jupiter` is a declared dependency but no `src/test` exists) — "testing" a use case means running it against a live Aerospike cluster and eyeballing console output / AQL results (see "Testing a use case" below).

Useful CLI flags (see `--help` for the full list, or `AerospikeConnector`/`BatchExecutor` for the option definitions):
- `-uc, --useCaseName <name>` — run a specific use case non-interactively (partial name match allowed); omit for the interactive menu
- `-l, --listUseCases` — list all registered use cases and their parameters
- `-p, --parameters` (with `-uc`) — show configurable parameters for one use case
- `-ro, --runOnly` / `-so, --seedOnly` — skip `setup()` or skip `run()` respectively (mutually exclusive)
- `--param.<name>=<value>` — override a use case's `Parameter<T>` from the command line (case-insensitive)
- `-Ddemo.namespace=<name>` (JVM system property, default `test`) — namespace all use cases read/write

In the interactive menu (`InteractiveMenu`/`UseCaseCookbookRunner`), use cases can also be filtered with `search <term>` / `s <term>` (text) or `/<regex>` (regex), and cleared with `clear`/`c`. See `source/README_SEARCH.md` for details.

## Architecture

**Entry point:** `UseCaseCookbookRunner.main()` parses CLI options, connects to Aerospike via `AerospikeConnector`, and then either runs one use case in batch mode (`BatchExecutor`) or launches `InteractiveMenu` for the menu-driven experience. Both paths build a single `IAerospikeClient` + `AeroMapper` pair and hand them to every use case.

**Cluster capability shim:** On startup, `UseCaseCookbookRunner.validateCluster()` checks the cluster's build version and whether the demo namespace (default `test`) has `strong-consistency` enabled — transactions require Aerospike 8+ with a strong-consistency namespace (enterprise feature). If the cluster doesn't meet that bar, the real client is wrapped in `AerospikeClientProxy.wrap()`, a dynamic proxy that strips `txn` from every `Policy` and no-ops `commit`/`abort`. This lets transaction-based use cases run (with transactional guarantees silently disabled) against any cluster, so use cases should not assume transactions are actually enforced.

**The `UseCase` interface** (`UseCase.java`) is the contract every example implements: `getName()`, `getDescription()` (searchable), `getReference()` (link to the matching `UseCases/*.md` doc), `getTags()`, `getParams()`, `setup(client, mapper)` (truncate/seed data), and `run(client, mapper)` (execute and print results). Implementations live one-per-package under `source/src/main/java/com/aerospike/examples/<package>/`, typically with a `model/` subpackage for their POJOs (e.g. `gaming/`, `hotkeys/`, `onetomany/`, `recordversioning/`).

**Registration:** every `UseCase` must be added to the static list in `UseCaseRegistry.java` — this is the single source of truth for what appears in the menu, batch mode, and `--listUseCases`. `UseCaseExecutor` drives one use case's lifecycle (interactive parameter editing, then `setup()`/`run()`), while `BatchExecutor` + `ParameterParser` do the equivalent for the non-interactive CLI path (`-uc`, `--param.*`).

**Parameters:** `Parameter<T>` is a simple mutable named value (name, default, description) that use cases expose via `getParams()` so users can tune things like record counts or run duration without editing code — from the interactive menu, from `--param.<name>=<value>`, or left at their default. Both `UseCaseExecutor` and `ParameterParser` mutate the private `value` field via reflection, so `Parameter` itself stays immutable-looking but is not thread-safe to mutate concurrently.

**`Async` library:** long-running simulations (hot-key tests, inventory sims, etc.) use `Async.runFor(Duration, mainThread -> { ... })` inside `run()`. Within that callback:
- `async.continuous(n, runnable)` — run `runnable` in a tight loop on `n` threads until the duration elapses
- `async.periodic(period, n, runnable)` — run `runnable` every `period` on `n` threads
- `async.virtualTime()`/`virtualDate()` — simulate a compressed/expanded time axis for generating realistic-looking historical data quickly

**Data modeling stack:** POJOs combine three libraries so use cases can seed realistic data with minimal boilerplate:
- **Lombok** (`@Data`, `@NoArgsConstructor`, `@AllArgsConstructor`) for getters/setters/constructors
- **Aerospike Java Object Mapper** (`@AerospikeRecord(namespace=, set=)`, `@AerospikeKey`) to map POJOs directly to records — used for `mapper.save()`/`mapper.read()` calls in use case code, but use cases intentionally also show raw `IAerospikeClient` operations (bins, keys, expressions) side-by-side for teaching purposes
- **Aerospike Java Object Generator** (`@GenMagic`, `@GenNumber`, `@GenExpression`, `@GenList`, etc.) driving `new Generator(Class).generate(startId, endId, threads, Class, consumer).monitor()` to bulk-seed data with plausible values

Bin names generated from POJO fields are subject to Aerospike's 15-character bin name limit — keep field names short.

## Adding a new use case

Full walkthrough with a complete worked example (an "Inventory Management System") is in `CONTRIBUTING.md` — read it before adding one. The short version:

1. Create POJOs in `source/src/main/java/com/aerospike/examples/<package>/model/` using the Lombok/Mapper/Generator annotations above.
2. Implement `UseCase` in `source/src/main/java/com/aerospike/examples/<package>/`. Put namespace/set lookups (`mapper.getNamespace(Class)`, `mapper.getSet(Class)`) in a private helper called from **both** `setup()` and `run()` — `run()` must work standalone when invoked with `--runOnly`, so it can't rely on state `setup()` would normally initialize.
3. Add the new class to `UseCaseRegistry.USE_CASES`.
4. Write `UseCases/<name>.md` documenting the scenario, data model, code walkthrough, and AQL examples; point `getReference()` at it, and link back to the source from the doc.
5. Add a short entry + link under "Use Cases" in `README.md`.

### Testing a use case end-to-end

```bash
cd source && mvn clean package
java -jar target/use-case-cookbook-*.jar --usecase="<name>" --param.<name>=<value>
```
Then inspect the seeded/updated data with `aql` against the `test` namespace (sets are prefixed `uccb_` to avoid clashing with other data) and confirm behavior with `--runOnly` alone (setup already done) as well as the full setup+run path.