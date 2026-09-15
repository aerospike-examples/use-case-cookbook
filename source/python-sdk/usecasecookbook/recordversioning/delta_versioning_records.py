"""Port of the legacy DeltaVersioningRecords (see ../../java and ../../java-sdk). Same
TradeBase live-record-plus-``versions``-map model as VersioningRecords, but instead of
copying the entire prior record to a historical key on every change, each update writes
a small *delta* audit record (``id:version``) describing only which bins changed and how
(``Inserted``/``Changed``/``TypeChanged``/``Removed``/``Same``), plus their new values -
enough to reconstruct any past version by replaying deltas from version 0 forward.

Deliberate simplification vs. the Java SDK port: that port classifies bin-level deltas
entirely server-side, in one ``operate()`` call, via a snapshot-before/apply/compare-after
sequence of ``Exp``/``MapExp`` write-and-read expressions (see its own extensive class
docstring for the SDK-specific parameter errors that forced it down that path instead of
AEL). This port instead reads the record before and after applying the caller's own
change, and diffs the two bin dicts in plain Python - one extra round trip (read-before,
write, read-after, vs. the Java port's single combined ``operate()`` call), but far
simpler and doesn't bet a correctness-critical diff on this SDK's own expression-write
composition, which is still only lightly verified for this kind of nested conditional
logic (see ``../timeseries``'s and ``../gaming``'s own AEL/expression gaps). A full-record
diff (every bin present in either snapshot) also replaces the Java port's
narrower "only bins the caller's own operations touched" diff - behaviorally equivalent
here, since nothing else in the record changes between the two reads, but simpler to
reason about since it doesn't need to inspect the caller's operations to know what to
compare.

``change_handler`` mirrors ``versioning_records.ChangeHandler``'s contract: it receives the
pre-change bins and the write builder already targeting the effective key, and returns
that builder extended with its own bin changes - this lets a caller express a blind
increment (``.add(...)``) without knowing the resulting value, same as the Java version's
``List<Operation>`` parameter.
"""

from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from aerospike_sdk import DataSet, SyncSession

from usecasecookbook import config
from usecasecookbook.recordversioning.model import TradeBase
from usecasecookbook.txn import run_in_transaction
from usecasecookbook.use_case import UseCase

NUM_RECORDS = 100
SOURCE_SYSTEMS = ["FIX", "MANUAL", "BLOOMBERG", "REUTERS"]
BOOKS = ["EQ-DESK-1", "FX-DESK-2", "RATES-DESK-3", "CREDIT-DESK-4"]
COUNTERPARTIES = ["Acme Capital", "Northwind Bank", "Contoso Securities", "Fabrikam Trading"]
PROTECTED_BINS = {"version", "versions", "updatedDate"}

TRADE_BASES = DataSet.of(config.NAMESPACE, "uccb_tradebase")

ChangeHandler = Callable[[Dict[str, Any], object], object]


def _now_millis() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _delta_key(trade_id: int, version: int):
    return TRADE_BASES.id(f"{trade_id}:{version}")


def _random_trade_base(trade_id: int) -> TradeBase:
    import random

    now = _now_millis()
    trade_date = now - random.randint(0, 30 * 24 * 60 * 60 * 1000)
    return TradeBase(
        id=trade_id,
        source_system_id=random.choice(SOURCE_SYSTEMS),
        parent_trade_id=0,
        ext_trade_id=f"EXT-{random.randint(0, 999_999)}",
        content_id=trade_id,
        book=random.choice(BOOKS),
        counterparty=random.choice(COUNTERPARTIES),
        trade_date=trade_date,
        entered_date=trade_date,
        trade_version=0,
        record_complete=False,
    )


def _next_versions_map(versions: Dict[Any, int], current_version: int, change_ts: float) -> Dict[Any, int]:
    """Closes the entry currently marked -1 (setting it to the version being closed out)
    and opens a new one, marked -1, at ``change_ts`` - or at ``old_key + 1`` if that would
    collide with (or not sort after) the entry being closed, which happens when successive
    updates to the same trade land in the same millisecond.
    """
    new_versions = dict(versions)
    old_key = next((k for k, v in new_versions.items() if v == -1), None)
    if old_key is None:
        new_versions[change_ts] = -1
        return new_versions
    new_key = max(change_ts, old_key + 1)
    new_versions[old_key] = current_version
    new_versions[new_key] = -1
    return new_versions


def _classify_and_diff(before: Dict[str, Any], after: Dict[str, Any]) -> List[Dict[str, Any]]:
    changes: List[Dict[str, Any]] = []
    for name in sorted(set(before) | set(after)):
        if name in PROTECTED_BINS:
            continue
        has_before = before.get(name) is not None
        has_after = after.get(name) is not None
        if not has_before and not has_after:
            continue
        if not has_before:
            changes.append({"binName": name, "status": "Inserted", "newValue": after[name]})
        elif not has_after:
            changes.append({"binName": name, "status": "Removed"})
        elif type(before[name]) is not type(after[name]):
            changes.append({"binName": name, "status": "TypeChanged", "newValue": after[name]})
        elif before[name] != after[name]:
            changes.append({"binName": name, "status": "Changed", "newValue": after[name]})
        # else: Same - not recorded.
    return changes


class DeltaVersioningRecords(UseCase):
    def get_name(self) -> str:
        return "Delta Versioning Records"

    def get_description(self) -> str:
        return (
            "Maintain an audit trail of TradeBase record changes using delta records instead of full "
            "copies. The initial insert is recorded as delta version 0 with all bins marked Inserted. "
            "Subsequent changes classify each touched bin as Inserted, Changed, TypeChanged, Removed, "
            "or Same, storing only the non-Same entries (with their new values) so any past version "
            "can be reconstructed by replaying deltas from version 0 forward."
        )

    def get_reference(self) -> str:
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/versioning-records-delta.md"

    def _update_trade_base_with_delta(
        self, session: SyncSession, trade_id: int, timestamp: float, description: str, user: str,
        change_handler: ChangeHandler,
    ) -> int:
        def _op(tx: SyncSession) -> int:
            key = TRADE_BASES.id(trade_id)
            row = tx.query(key).execute().first()
            before = dict(row.record.bins) if row is not None and row.record is not None else {}

            current_version = before.get("version", -1)
            new_version = current_version + 1
            ts_to_use = timestamp if timestamp else _now_millis()

            builder = change_handler(before, tx.upsert(key))
            builder = (
                builder.bin("version").set_to(new_version)
                .bin("updatedDate").set_to(ts_to_use)
                .bin("versions").set_to(_next_versions_map(before.get("versions") or {}, current_version, ts_to_use))
            )
            builder.execute()

            after_row = tx.query(key).execute().first()
            after = dict(after_row.record.bins) if after_row is not None and after_row.record is not None else {}
            changes = _classify_and_diff(before, after)

            tx.insert(_delta_key(trade_id, new_version)).bin("description").set_to(description) \
                .bin("user").set_to(user) \
                .bin("changeTs").set_to(ts_to_use) \
                .bin("deltaVer").set_to(new_version) \
                .bin("changes").set_to(changes) \
                .execute()

            return new_version

        return run_in_transaction(session, _op)

    def _get_audit_trail(self, session: SyncSession, trade_id: int) -> List[Dict[str, Any]]:
        current = session.query(TRADE_BASES.id(trade_id)).execute().first()
        current_version = current.record.bins["version"]
        trail = []
        for version in range(current_version + 1):
            row = session.query(_delta_key(trade_id, version)).execute().first()
            if row is not None and row.record is not None:
                trail.append(row.record.bins)
        return trail

    def _reconstruct_at_version(self, session: SyncSession, trade_id: int, target_version: int) -> Dict[str, Any]:
        reconstructed: Dict[str, Any] = {}
        for version in range(target_version + 1):
            row = session.query(_delta_key(trade_id, version)).execute().first()
            if row is None or row.record is None:
                raise ValueError(f"Missing delta record for version {version}")
            for change in row.record.bins.get("changes") or []:
                bin_name = change["binName"]
                if change["status"] == "Removed":
                    reconstructed.pop(bin_name, None)
                else:
                    reconstructed[bin_name] = change.get("newValue")
        reconstructed["version"] = target_version
        return reconstructed

    def _print_audit_trail(self, session: SyncSession, trade_id: int) -> None:
        print(f"Audit trail for TradeBase id {trade_id}:")
        for bins in self._get_audit_trail(session, trade_id):
            print(f"  Delta v{bins['deltaVer']} at {bins['changeTs']} by {bins['user']}: {bins['description']}")
            for change in bins.get("changes") or []:
                print(f"    {change['binName']}: {change['status']}")

    def setup(self, session: SyncSession) -> None:
        session.truncate(TRADE_BASES)

        print(f"Generating {NUM_RECORDS:,} trades")
        for trade_id in range(NUM_RECORDS):
            initial_bins = _random_trade_base(trade_id).to_bins()
            initial_bins.pop("version", None)
            initial_bins.pop("versions", None)

            def _insert_handler(_before: Dict[str, Any], builder: object, _bins=initial_bins) -> object:
                for name, value in _bins.items():
                    if value is not None:
                        builder = builder.bin(name).set_to(value)
                return builder

            self._update_trade_base_with_delta(
                session, trade_id, _now_millis(), "Initial insert", "setup", _insert_handler,
            )

    def run(self, session: SyncSession) -> None:
        trade_id = 2

        # An increment - the caller never learns (or needs to supply) the resulting value;
        # the before/after diff still correctly classifies it as Changed (or Same, on the
        # rare chance an increment lands back on the same value).
        self._update_trade_base_with_delta(
            session, trade_id, 0, "Increment trade version", "batch-user",
            lambda before, builder: builder.bin("tradeVersion").add(1),
        )

        self._update_trade_base_with_delta(
            session, trade_id, 0, "Update counterparty and book", "alice",
            lambda before, builder: builder.bin("counterparty").set_to("CP-1001").bin("book").set_to("XY"),
        )

        self._update_trade_base_with_delta(
            session, trade_id, 0, "Mark record complete", "bob",
            lambda before, builder: builder.bin("recordComplete").set_to(True),
        )

        self._print_audit_trail(session, trade_id)

        print("\nVersion map:")
        versions_row = session.query(TRADE_BASES.id(trade_id)).bins(["versions"]).execute().first()
        print(versions_row.record.bins.get("versions") if versions_row and versions_row.record else None)

        current_row = session.query(TRADE_BASES.id(trade_id)).execute().first()
        version = current_row.record.bins["version"]
        print(f"\nReconstructed at version {version}:")
        print(self._reconstruct_at_version(session, trade_id, version))
