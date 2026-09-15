"""Mirrors ../../../java-sdk's hotkeys/HotKeyProductSetup.java. Seeds the shared
HotKeyProduct records used by all hot-key use cases.
"""

from aerospike_async import Key
from aerospike_sdk import SyncSession

from usecasecookbook.hotkeys import keys as hot_key_keys
from usecasecookbook.hotkeys.simulation import HOT_PRODUCT_ID
from usecasecookbook.hotkeys.model import HotKeyProduct


def truncate_and_seed(session: SyncSession, replica_count: int) -> None:
    session.truncate(hot_key_keys.PRODUCTS)

    product = HotKeyProduct(HOT_PRODUCT_ID, "SKU-1000", "Generic demo product for hot-key simulations", 0)

    # The primary key matches product.id, so it can be written as a plain object-shaped put.
    session.upsert(hot_key_keys.primary(HOT_PRODUCT_ID)).put(product.to_bins()).execute()

    # Replica keys use a custom "productId:index" id, not product.id - object mapping always
    # derives the key from the model's own id, so these still need raw bin writes.
    for i in range(replica_count):
        session.upsert(hot_key_keys.replica(HOT_PRODUCT_ID, i)).put(product.to_bins()).execute()


def read_units_sold(session: SyncSession, key: Key) -> int:
    """Reads ``unitsSold`` from a single key, failing clearly if the record isn't seeded yet."""
    stream = session.query(key).execute()
    try:
        result = stream.first()
        if result is None or not result.is_ok or result.record is None:
            raise RuntimeError(f"No product record at key {key} - run setup() before run().")
        return result.record.bins["unitsSold"]
    finally:
        stream.close()


def read_merged_units_sold(session: SyncSession, replica_count: int) -> int:
    """Batch-reads ``unitsSold`` from every shard key (``productId:0..N-1``) and returns the
    sum - the logical total after sharded writes.
    """
    key_list = hot_key_keys.all_replica_keys(HOT_PRODUCT_ID, replica_count)
    total = 0
    stream = session.query(key_list).bins(["unitsSold"]).execute()
    try:
        for row in stream:
            if row.is_ok and row.record is not None:
                total += row.record.bins["unitsSold"]
    finally:
        stream.close()
    return total


def increment_units_sold_on_all_copies(session: SyncSession, replica_count: int) -> None:
    """Increments ``unitsSold`` on the primary key and every replica in a single batch write so
    all copies stay in sync.
    """
    key_list = hot_key_keys.primary_and_all_replica_keys(HOT_PRODUCT_ID, replica_count)
    stream = session.upsert(key_list).bin("unitsSold").add(1).execute()
    stream.close()
