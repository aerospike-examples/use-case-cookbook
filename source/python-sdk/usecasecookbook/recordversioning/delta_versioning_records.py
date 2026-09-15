"""Port of the legacy DeltaVersioningRecords (see ../../java and ../../java-sdk). Same
TradeBase live-record-plus-``versions``-map model as VersioningRecords, but instead of
copying the entire prior record to a historical key on every change, each update writes
a small *delta* audit record (``id:version``) describing only which bins changed and how
(``Inserted``/``Changed``/``TypeChanged``/``Removed``/``Same``), plus their new values -
enough to reconstruct any past version by replaying deltas from version 0 forward.

Like the Java SDK port, this classifies bin-level deltas entirely server-side in one
``operate()``-style call: for each bin the caller's change touches, snapshot its current
value into a temporary bin (``_t0``, ``_t1``, ...) via an expression write, apply the
caller's own operations unmodified, then compare each temp snapshot against the bin's new
value via an expression read, writing the classification into a result label (``_a0``,
``_a1``, ...). The live-record update and the delta audit record insert remain two
round trips total, matching the Java port - not three, which a naive
read-before/write/read-after client-side diff would need.

``changed_bins``/``apply_ops`` is this port's equivalent of the Java version's
``List<Operation> userOps`` parameter: the caller declares which bin names its change will
touch (so the snapshot/compare expressions can be built ahead of the write) and supplies a
callback that chains the actual write operations onto the builder - which can be a blind
increment (``.add(...)``) or any other CDT write, without the caller ever needing to know
the resulting value up front. Python has no standalone ``Operation`` value type to inspect
bin names from the way Java does, so the bin list is passed explicitly instead of derived.

This SDK's AEL grammar has no write-shaped terminals (see ``../README.md``), so the
server-side snapshot/compare/versions-bookkeeping expressions here are built with
``aerospike_sdk.Exp`` (``FilterExpression``) instead - a near-complete
``Exp``/``MapExp``/``ListExp``-equivalent builder, confirmed by testing against a live
cluster rather than assumed. One read (the pre-update ``version`` value, needed to number
the delta record even though most of the session doesn't otherwise need it) is expressed
as a genuine AEL string (``.select_from("when(...)")``) alongside the ``Exp``-based writes
in the same call, demonstrating - as the Java port's own reviewer asked for - that AEL and
expression operations can be freely mixed within one call rather than a call needing to be
one or the other.
"""

from collections.abc import Callable
from datetime import datetime, timezone

from aerospike_async import MapOrder, MapPolicy, MapWriteMode
from aerospike_sdk import (
    DataSet,
    Exp,
    ExpType,
    ListReturnType,
    MapReturnType,
    SyncSession,
)

from usecasecookbook import config
from usecasecookbook.recordversioning.model import TradeBase
from usecasecookbook.txn import run_in_transaction
from usecasecookbook.use_case import UseCase

NUM_RECORDS = 100
SOURCE_SYSTEMS = ["FIX", "MANUAL", "BLOOMBERG", "REUTERS"]
BOOKS = ["EQ-DESK-1", "FX-DESK-2", "RATES-DESK-3", "CREDIT-DESK-4"]
COUNTERPARTIES = ["Acme Capital", "Northwind Bank", "Contoso Securities", "Fabrikam Trading"]

TRADE_BASES = DataSet.of(config.NAMESPACE, "uccb_tradebase")
VERSIONS_MAP_POLICY = MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE)

ApplyOps = Callable[[object], object]

# Aerospike's server-side particle-type codes, needed for the bin_type() comparisons
# below - confirmed empirically against a live cluster, since this SDK doesn't expose a
# public constant set for these the way the Java client's
# com.aerospike.client.sdk.command.ParticleType does.
_INTEGER, _FLOAT, _STRING, _BOOL = 1, 2, 3, 17

_TYPED_BIN_GETTERS = {
    _INTEGER: Exp.int_bin,
    _FLOAT: Exp.float_bin,
    _STRING: Exp.string_bin,
    _BOOL: Exp.bool_bin,
}

# Hard-coded mapping of TradeBase bin names to Aerospike particle types (dates as INTEGER).
BIN_PARTICLE_TYPES: dict[str, int] = {
    "id": _INTEGER,
    "sourceSystemId": _STRING,
    "parentTradeId": _INTEGER,
    "extTradeId": _STRING,
    "contentId": _INTEGER,
    "book": _STRING,
    "counterparty": _STRING,
    "tradeDate": _INTEGER,
    "enteredDate": _INTEGER,
    "tradeVersion": _INTEGER,
    "recordComplete": _BOOL,
    "dataVersion": _INTEGER,
}


def _now_millis() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _delta_key(trade_id: int, version: int):
    return TRADE_BASES.id(f"{trade_id}:{version}")


def _temp_bin(index: int) -> str:
    return f"_t{index}"


def _action_bin(index: int) -> str:
    return f"_a{index}"


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


def _current_version_exp() -> Exp:
    """The live record's current ``version``, or -1 if the record (and hence the bin) is new."""
    return Exp.cond([Exp.not_(Exp.bin_exists("version")), Exp.int_val(-1), Exp.int_bin("version")])


def _snapshot_exp(bin_name: str) -> Exp:
    """Copies the current value of a bin into a temporary bin before the caller's
    operations run. Writes ``unknown()`` (a no-op under ``ignore_eval_failure``) when the
    bin is absent or its particle type doesn't match the expected type, so the subsequent
    comparison classifies the change correctly.
    """
    particle_type = BIN_PARTICLE_TYPES[bin_name]
    typed_get = _TYPED_BIN_GETTERS[particle_type]
    return Exp.cond([
        Exp.not_(Exp.bin_exists(bin_name)), Exp.unknown(),
        Exp.eq(Exp.bin_type(bin_name), Exp.int_val(particle_type)), typed_get(bin_name),
        Exp.unknown(),
    ])


def _compare_exp(bin_name: str, temp_bin: str) -> Exp:
    """Compares a bin's value after the caller's operations with the temporary snapshot
    taken beforehand, evaluating to ``Inserted``, ``Changed``, ``TypeChanged``,
    ``Removed``, or ``Same``.
    """
    particle_type = BIN_PARTICLE_TYPES[bin_name]
    typed_get = _TYPED_BIN_GETTERS[particle_type]
    return Exp.cond([
        Exp.not_(Exp.bin_exists(bin_name)), Exp.string_val("Removed"),
        Exp.not_(Exp.bin_exists(temp_bin)), Exp.string_val("Inserted"),
        Exp.ne(Exp.bin_type(bin_name), Exp.bin_type(temp_bin)), Exp.string_val("TypeChanged"),
        Exp.eq(typed_get(bin_name), typed_get(temp_bin)), Exp.string_val("Same"),
        Exp.string_val("Changed"),
    ])


def _versions_exp(change_ts: int) -> Exp:
    """Computes the ``versions`` map's new value: closes the entry currently marked -1
    (setting it to the version being closed out) and opens a new one, marked -1, at
    ``change_ts`` - or at ``old_key + 1`` if that would collide with (or not sort after)
    the entry being closed, which happens when successive updates to the same trade land
    in the same millisecond. Short-circuits to a fresh one-entry map when ``versions``
    doesn't exist yet.
    """
    close_and_reopen = Exp.exp_let([
        Exp.def_("oldKeyList", Exp.map_get_by_value(MapReturnType.KEY, Exp.int_val(-1), Exp.map_bin("versions"), [])),
        Exp.def_("hasOldKey", Exp.gt(Exp.list_size(Exp.var("oldKeyList"), []), Exp.int_val(0))),
        Exp.def_("oldKey", Exp.cond([
            Exp.var("hasOldKey"),
            Exp.list_get_by_index(ListReturnType.VALUE, ExpType.INT, Exp.int_val(0), Exp.var("oldKeyList"), []),
            Exp.int_val(0),
        ])),
        Exp.def_("newKey", Exp.cond([
            Exp.var("hasOldKey"),
            Exp.max([Exp.int_val(change_ts), Exp.num_add([Exp.var("oldKey"), Exp.int_val(1)])]),
            Exp.int_val(change_ts),
        ])),
        Exp.cond([
            Exp.var("hasOldKey"),
            Exp.map_put(
                VERSIONS_MAP_POLICY, Exp.var("newKey"), Exp.int_val(-1),
                Exp.map_put(VERSIONS_MAP_POLICY, Exp.var("oldKey"), _current_version_exp(), Exp.map_bin("versions"), []),
                [],
            ),
            Exp.map_put(VERSIONS_MAP_POLICY, Exp.var("newKey"), Exp.int_val(-1), Exp.map_bin("versions"), []),
        ]),
    ])
    fresh_map = Exp.map_put(VERSIONS_MAP_POLICY, Exp.int_val(change_ts), Exp.int_val(-1), Exp.map_val({}), [])
    return Exp.cond([Exp.not_(Exp.bin_exists("versions")), fresh_map, close_and_reopen])


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
        changed_bins: list[str], apply_ops: ApplyOps,
    ) -> int:
        def _op(tx: SyncSession) -> int:
            key = TRADE_BASES.id(trade_id)
            change_ts = int(timestamp) if timestamp else _now_millis()

            # The one genuinely AEL-expressible piece of this call: the pre-update
            # "version" value, read before the Exp write below touches it (an AEL read of
            # a bin an Exp op already wrote in the same call fails validation; the reverse
            # order doesn't).
            builder = tx.upsert(key).bin("version_old").select_from(
                "when($.version.exists() == false => -1, default => $.version)"
            )
            for index, bin_name in enumerate(changed_bins):
                builder = builder.bin(_temp_bin(index)).upsert_from(
                    _snapshot_exp(bin_name), ignore_eval_failure=True, ignore_op_failure=True,
                )

            builder = apply_ops(builder)

            for index, bin_name in enumerate(changed_bins):
                builder = (
                    builder.bin(_action_bin(index)).select_from(
                        _compare_exp(bin_name, _temp_bin(index)), ignore_eval_failure=True,
                    )
                    .bin(bin_name).get()
                )
            for index in range(len(changed_bins)):
                builder = builder.bin(_temp_bin(index)).set_to(None)

            builder = (
                builder.bin("versions").upsert_from(_versions_exp(change_ts))
                .bin("version").upsert_from(Exp.num_add([_current_version_exp(), Exp.int_val(1)]))
                .bin("updatedDate").set_to(change_ts)
            )

            result = builder.execute().first()
            bins = result.record.bins

            changes: list[dict[str, object]] = []
            for index, bin_name in enumerate(changed_bins):
                status = bins.get(_action_bin(index))
                if status is None or status == "Same":
                    continue
                change: dict[str, object] = {"binName": bin_name, "status": status}
                if status != "Removed":
                    change["newValue"] = bins.get(bin_name)
                changes.append(change)

            new_version = int(bins["version_old"]) + 1

            tx.insert(_delta_key(trade_id, new_version)) \
                .bin("description").set_to(description) \
                .bin("user").set_to(user) \
                .bin("changeTs").set_to(change_ts) \
                .bin("deltaVer").set_to(new_version) \
                .bin("changes").set_to(changes) \
                .execute()

            return new_version

        return run_in_transaction(session, _op)

    def _get_audit_trail(self, session: SyncSession, trade_id: int) -> list[dict[str, object]]:
        current = session.query(TRADE_BASES.id(trade_id)).execute().first()
        current_version = current.record.bins["version"]
        trail = []
        for version in range(current_version + 1):
            row = session.query(_delta_key(trade_id, version)).execute().first()
            if row is not None and row.record is not None:
                trail.append(row.record.bins)
        return trail

    def _reconstruct_at_version(self, session: SyncSession, trade_id: int, target_version: int) -> dict[str, object]:
        reconstructed: dict[str, object] = {}
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
            initial_bins.pop("updatedDate", None)
            changed_bins = list(initial_bins.keys())

            def _insert_ops(builder: object, _bins=initial_bins) -> object:
                for name, value in _bins.items():
                    builder = builder.bin(name).set_to(value)
                return builder

            self._update_trade_base_with_delta(
                session, trade_id, _now_millis(), "Initial insert", "setup", changed_bins, _insert_ops,
            )

    def run(self, session: SyncSession) -> None:
        trade_id = 2

        # An increment - the caller never learns (or needs to supply) the resulting value;
        # the server-side snapshot/compare still correctly classifies it as Changed (or
        # Same, on the rare chance an increment lands back on the same value).
        self._update_trade_base_with_delta(
            session, trade_id, 0, "Increment trade version", "batch-user",
            ["tradeVersion"], lambda builder: builder.bin("tradeVersion").add(1),
        )

        self._update_trade_base_with_delta(
            session, trade_id, 0, "Update counterparty and book", "alice",
            ["counterparty", "book"],
            lambda builder: builder.bin("counterparty").set_to("CP-1001").bin("book").set_to("XY"),
        )

        self._update_trade_base_with_delta(
            session, trade_id, 0, "Mark record complete", "bob",
            ["recordComplete"], lambda builder: builder.bin("recordComplete").set_to(True),
        )

        self._print_audit_trail(session, trade_id)

        print("\nVersion map:")
        versions_row = session.query(TRADE_BASES.id(trade_id)).bins(["versions"]).execute().first()
        print(versions_row.record.bins.get("versions") if versions_row and versions_row.record else None)

        current_row = session.query(TRADE_BASES.id(trade_id)).execute().first()
        version = current_row.record.bins["version"]
        print(f"\nReconstructed at version {version}:")
        print(self._reconstruct_at_version(session, trade_id, version))
