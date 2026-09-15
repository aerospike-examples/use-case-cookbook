"""Port of the legacy VersioningRecords (see ../../java and ../../java-sdk). Maintains
historical versions of records: the "effective" record (unversioned key, e.g.
``trade:12345``) always reflects the latest data, and each update first copies the
current effective record verbatim (except its ``versions`` bin) to a new immutable
historical record keyed ``trade:12345:N`` before applying the change - all inside one
transaction. The effective record's ``versions`` bin is a key-ordered map from update
timestamp to the version number that was effective starting at that time (the newest
entry is always version ``-1``, meaning "look at the effective record itself, not a
historical one").

Each caller-supplied ``change_handler`` receives the record as read inside the
transaction and the write builder already targeting the effective key, and returns that
builder extended with its own bin changes chained on - ``version``/``updatedDate``/
``versions`` are then chained on top of whatever the handler added.

Unlike the Java SDK port, this SDK's write-side CDT builder exposes
``on_map_key_relative_index_range`` on both the read and write sides (no asymmetry), so
``read_at_time`` uses a plain read-only query rather than routing a read through the
write builder as a workaround.
"""

import random
from datetime import datetime, timezone
from typing import Callable, Dict, Optional

from aerospike_async import MapOrder
from aerospike_sdk import DataSet, SyncSession

from usecasecookbook import config
from usecasecookbook.async_util import Async
from usecasecookbook.recordversioning.model import TradeBase, TradeStaticData
from usecasecookbook.txn import run_in_transaction
from usecasecookbook.use_case import UseCase

NUM_TRADES = 10_000
SOURCE_SYSTEMS = ["FIX", "MANUAL", "BLOOMBERG", "REUTERS"]
BOOKS = ["EQ-DESK-1", "FX-DESK-2", "RATES-DESK-3", "CREDIT-DESK-4"]
COUNTERPARTIES = ["Acme Capital", "Northwind Bank", "Contoso Securities", "Fabrikam Trading"]

TRADE_BASES = DataSet.of(config.NAMESPACE, "uccb_tradebase")
TRADE_CONTENTS = DataSet.of(config.NAMESPACE, "uccb_tradecontent")

ChangeHandler = Callable[[dict, object], object]


def _form_key(dataset: DataSet, trade_id: int, version: Optional[int] = None):
    if version is None or version < 0:
        return dataset.id(trade_id)
    return dataset.id(f"{trade_id}:{version}")


def _now_millis() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _random_trade_content(trade_id: int) -> TradeStaticData:
    words = " ".join(f"word{random.randint(0, 999)}" for _ in range(50))
    return TradeStaticData(trade_id=trade_id, version=0, data=words, mutable_data=0)


def _random_trade_base(trade_id: int) -> TradeBase:
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
        record_complete=True,
    )


class VersioningRecords(UseCase):
    def get_name(self) -> str:
        return "Versioned Records"

    def get_description(self) -> str:
        return (
            "Maintain historical versions of records with point-in-time query capabilities. "
            "Demonstrates atomic version creation using transactions and time-based queries "
            "using map operations. Objects are assumed to have 2 parts -- a base record which "
            "changes frequently and is small, and a details record which is large and changes "
            "infrequently."
        )

    def get_reference(self) -> str:
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/versioning-records.md"

    def setup(self, session: SyncSession) -> None:
        session.truncate(TRADE_BASES)
        session.truncate(TRADE_CONTENTS)

        print(f"Generating {NUM_TRADES:,} trades")
        now = _now_millis()
        for trade_id in range(NUM_TRADES):
            content = _random_trade_content(trade_id)
            trade = _random_trade_base(trade_id)
            trade.version = 0
            trade.data_version = content.version
            # The current version is always -1.
            trade.versions = {now: -1}
            trade.updated_date = now

            session.upsert(TRADE_CONTENTS.id(content.trade_id)).put(content.to_bins()).execute()
            session.upsert(TRADE_BASES.id(trade.id)).put(trade.to_bins()).execute()

    def _update_object_with_version(
        self, session: SyncSession, dataset: DataSet, trade_id: int, timestamp: float,
        handler: ChangeHandler,
    ) -> int:
        def _op(tx: SyncSession) -> int:
            unversioned_key = _form_key(dataset, trade_id)
            row = tx.query(unversioned_key).execute().first()
            bins = row.record.bins

            current_version = bins["version"]
            versions_bin: Optional[Dict] = bins.get("versions")

            # Copy the current effective record (except "versions") to a new historical,
            # immutable record.
            versioned_key = _form_key(dataset, trade_id, current_version)
            copy_builder = tx.insert(versioned_key)
            for name, value in bins.items():
                if name != "versions":
                    copy_builder = copy_builder.bin(name).set_to(value)
            copy_builder.execute()

            # Apply the caller's changes, then bump version/updatedDate/versions on the
            # effective record.
            new_version = current_version + 1
            ts_to_use = timestamp if timestamp else _now_millis()

            update_builder = handler(bins, tx.update(unversioned_key))
            update_builder = update_builder.bin("version").set_to(new_version).bin("updatedDate").set_to(ts_to_use)

            if versions_bin:
                map_key_of_current_version = next(ts for ts, v in versions_bin.items() if v == -1)
                update_builder = (
                    update_builder.bin("versions").on_map_key(map_key_of_current_version).set_to(current_version)
                )
                update_builder = (
                    update_builder.bin("versions")
                    .on_map_key(ts_to_use, create_type=MapOrder.KEY_ORDERED)
                    .set_to(-1)
                )
            update_builder.execute()

            return new_version

        return run_in_transaction(session, _op)

    def _read_at_time(self, session: SyncSession, trade_id: int, timestamp: float) -> Optional[TradeBase]:
        unversioned_key = _form_key(TRADE_BASES, trade_id)

        row = (
            session.query(unversioned_key)
            .bin("versions").on_map_key_relative_index_range(timestamp + 1, -1, 1).get_keys_and_values()
            .execute()
            .first()
        )
        if row is None or row.record is None:
            raise ValueError(f"No trade base with id: {trade_id}")

        versions_result = row.record.bins.get("versions")
        if not versions_result:
            # Before the earliest recorded version.
            return None
        version = next(iter(versions_result.values()))

        if version == -1:
            found = session.query(unversioned_key).execute().first()
            return TradeBase.from_bins(found.record.bins)
        versioned_key = _form_key(TRADE_BASES, trade_id, int(version))
        found = session.query(versioned_key).execute().first()
        return TradeBase.from_bins(found.record.bins)

    def run(self, session: SyncSession) -> None:
        def _run(async_runner: Async) -> None:
            def _fast_update() -> None:
                trade_id = 2
                new_version = self._update_object_with_version(
                    session, TRADE_CONTENTS, trade_id, 0,
                    lambda rec, builder: builder.bin("mutableData").add(3),
                )
                self._update_object_with_version(
                    session, TRADE_BASES, trade_id, 0,
                    lambda rec, builder: builder.bin("dataVersion").add(new_version),
                )

            def _slow_update() -> None:
                self._update_object_with_version(
                    session, TRADE_BASES, 1, 0,
                    lambda rec, builder: builder.bin("tradeVersion").add(3),
                )

            async_runner.periodic(0.2, _fast_update)
            async_runner.periodic(1.0, _slow_update)

        Async.run_for(5.0, _run)

        now = _now_millis()
        print("Version map:")
        versions_row = session.query(_form_key(TRADE_BASES, 2)).bins(["versions"]).execute().first()
        print(versions_row.record.bins.get("versions") if versions_row and versions_row.record else None)

        print(f"\nReading current version (at {now}):")
        print(self._read_at_time(session, 2, now))
        print("\nReading current version 2 seconds ago:")
        print(self._read_at_time(session, 2, now - 2000))
        print("\nReading before the first version")
        print(self._read_at_time(session, 2, now - 200_000))
