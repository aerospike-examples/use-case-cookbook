"""Port of ../../java-sdk's TopTransactionsAcrossDcs (itself an SDK port of ../../java).

Transactions can arrive at either of two simulated DCs; each account keeps the most
recent ``MAX_TRANSACTIONS`` transaction ids per DC in a key-ordered map (``txns_dc1``/
``txns_dc2``, keyed by a zero-padded ``"timestamp-id"`` string so map key order == time
order), trimmed on every write via the native CDT builder (``on_map_key(...,
create_type=...).set_to(...)`` then ``on_map_index_range(-N).remove_all_others()`` - this
SDK's equivalent of ../../java-sdk's ``.onMapKey(...).upsert(...)``/
``.onMapIndexRange(-N).removeAllOthers()``). Reading an account's overall top transactions
means merging both DC maps and taking the most recent entries across both - see
:meth:`TopTransactionsAcrossDcs.get_top_results` for why that merge is done client-side
here rather than as a single AEL read the way ../../java-sdk does it.
"""

import random
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List

from aerospike_async import MapOrder
from aerospike_sdk import DataSet, SyncSession

from usecasecookbook import config
from usecasecookbook.async_util import Async
from usecasecookbook.transactionprocessing.model import Account, Transaction
from usecasecookbook.use_case import UseCase

BIN_DC1 = "txns_dc1"
BIN_DC2 = "txns_dc2"
NUM_ACCOUNTS = 1_000
SIMULATION_DAYS = 30
MAX_TRANSACTIONS = 50
RUNTIME_SECS = 25.0

DESCRIPTIONS = ["Grocery", "Gas station", "Online purchase", "ATM withdrawal", "Restaurant"]
STATUSES = ["APPROVED", "DENIED", "FRAUD"]

# Set names must match ../../java-sdk exactly - uccb_account is intentionally shared with
# manytomany's own (unrelated) Account model, mirroring a pre-existing collision on the
# Java SDK side rather than introducing a different name here.
ACCOUNTS = DataSet.of(config.NAMESPACE, "uccb_account")
TRANSACTIONS = DataSet.of(config.NAMESPACE, "uccb_txn")


class _Counter:
    """A plain int isn't safe to increment from multiple ``Async`` threads at once -
    this is the same role as ../../java-sdk's ``AtomicLong``.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._value = 0

    def increment(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    def get(self) -> int:
        with self._lock:
            return self._value


def _form_map_key(txn: Transaction) -> str:
    return f"{txn.timestamp:013d}-{txn.id:>8s}"


def _random_transaction(counter: int) -> Transaction:
    txn_id = f"txn-{counter}-{random.randint(0, 999_999)}"
    account_id = f"acct-{random.randint(1, NUM_ACCOUNTS)}"
    amount = random.randint(1, 99_999)
    status = random.choice(STATUSES)
    desc = random.choice(DESCRIPTIONS)
    approval_code = format(random.getrandbits(64), "x")
    return Transaction(txn_id, 0, amount, desc, status, None, approval_code, account_id)


class TopTransactionsAcrossDcs(UseCase):
    def get_name(self) -> str:
        return "Top 50 Transaction Across DCs"

    def get_description(self) -> str:
        return (
            "Find the top 50 transactions for an account. Transactions can arrive at either of 2 DCs at any point "
            "in time. Transaction shipping from the remote DC is about 100ms. Transactions have a unique id but 2 "
            "transaction can arrive for the same account at the same time."
        )

    def get_reference(self) -> str:
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/top-transactions-across-dcs.md"

    def setup(self, session: SyncSession) -> None:
        session.truncate(ACCOUNTS)
        session.truncate(TRANSACTIONS)

        print(f"Generating {NUM_ACCOUNTS:,} accounts")
        for i in range(1, NUM_ACCOUNTS + 1):
            account = Account(f"acct-{i}")
            session.upsert(ACCOUNTS.id(account.id)).put(account.to_bins()).execute()

    def run(self, session: SyncSession) -> None:
        def main_thread(async_runner: Async) -> None:
            total_txns = _Counter()
            txn_counter = _Counter()

            print(f"Starting simulating time for {RUNTIME_SECS:.0f} seconds")

            # We want the transactions to be roughly ordered to simulate real traffic patterns.
            # Given the run should be short, simulate SIMULATION_DAYS of transactions over
            # RUNTIME_SECS of wall-clock time, starting SIMULATION_DAYS in the (virtual) past.
            async_runner.use_virtual_time(
                timedelta(days=SIMULATION_DAYS),
                timedelta(seconds=RUNTIME_SECS),
                start_offset=timedelta(days=SIMULATION_DAYS),
            )

            print(f"Starting at: {async_runner.virtual_date()}")

            async_runner.periodic(
                2.5,
                lambda: self._show_top_transactions_for_account1(session, async_runner, total_txns),
            )

            def _generate() -> None:
                txn = _random_transaction(txn_counter.increment())
                if random.random() < 0.5:
                    # ~15ms either side of "now" - the local DC.
                    txn.timestamp = int(async_runner.virtual_time_with_variance(-0.015, 0.010) * 1000)
                    bin_name = BIN_DC1
                else:
                    # 10-100ms behind "now" - the remote DC, per get_description()'s ~100ms shipping time.
                    txn.timestamp = int(async_runner.virtual_time_with_variance(-0.100, -0.010) * 1000)
                    bin_name = BIN_DC2
                txn.origin = bin_name
                session.upsert(TRANSACTIONS.id(txn.id)).put(txn.to_bins()).execute()

                # Insert the new transaction into the account's DC map, and trim that map to
                # the most recent MAX_TRANSACTIONS entries.
                map_key = _form_map_key(txn)
                session.upsert(ACCOUNTS.id(txn.account_id)) \
                    .bin(bin_name).on_map_key(map_key, create_type=MapOrder.KEY_ORDERED).set_to(txn.id) \
                    .bin(bin_name).on_map_index_range(-MAX_TRANSACTIONS).remove_all_others() \
                    .execute()
                total_txns.increment()

            async_runner.continuous(_generate, number_of_copies=-1)

        Async.run_for(RUNTIME_SECS, main_thread)

    def _show_top_transactions_for_account1(
        self, session: SyncSession, async_runner: Async, total_txns: _Counter,
    ) -> None:
        print(f"{async_runner.virtual_date()}: {total_txns.get():,} transactions generated")
        start = time.monotonic()
        top_results = self.get_top_results(session, MAX_TRANSACTIONS, "acct-1")
        elapsed_ms = (time.monotonic() - start) * 1000
        for i, txn in enumerate(top_results, start=1):
            print(
                f"{i:4d}: {txn.id:>10s} {txn.account_id:>8s} {txn.origin:>10s}  "
                f"{datetime.fromtimestamp(txn.timestamp / 1000)}  ${txn.amount}"
            )
        print(f"{len(top_results)} transaction(s) retrieved in {elapsed_ms:,.0f}ms\n")

    def get_top_results(self, session: SyncSession, count: int, account_id: str) -> List[Transaction]:
        """Return an account's most recent transactions across both DC maps, newest first.

        ../../java-sdk merges the two per-DC maps and takes the top N as a single AEL read
        (``let (merged = $.dc1.putItems($.dc2)) then ((${merged}).{-N:})``, wrapped in a
        ``when`` for a DC bin that doesn't exist yet). This SDK's AEL grammar has no
        equivalent map-merge path function - confirmed via the ANTLR grammar (there is no
        ``putItems`` token anywhere in ``Condition.g4``) and empirically: running that exact
        expression through ``select_from`` raises ``AelParseException: line 1:28 no viable
        alternative at input 'let(merged=$.dc1.putItems('``. So the merge is done
        client-side here instead: read the account record (both DC maps come back as plain
        dicts keyed by the same zero-padded "timestamp-id" string used to write them, so
        string order == chronological order), merge the two dicts, sort the combined keys
        descending, and take the top few. Over-fetches by a few entries (same as
        ../../java-sdk) since a transaction's map entry can arrive slightly before the
        transaction record itself is written.
        """
        stream = session.query(ACCOUNTS.id(account_id)).execute()
        record = None
        for row in stream:
            if row.is_ok:
                record = row.record
        stream.close()
        if record is None:
            return []

        merged: Dict[str, str] = {}
        merged.update(record.bins.get(BIN_DC1) or {})
        merged.update(record.bins.get(BIN_DC2) or {})
        if not merged:
            return []

        count_to_use = count + 3
        top_keys = sorted(merged.keys(), reverse=True)[:count_to_use]
        txn_ids = [merged[k] for k in top_keys]

        keys = [TRANSACTIONS.id(tid) for tid in txn_ids]
        txn_stream = session.query(keys).execute()
        txns: List[Transaction] = []
        for row in txn_stream:
            if row.is_ok and row.record is not None:
                txns.append(Transaction.from_bins(row.record.bins))
        txn_stream.close()
        return txns[:count]
