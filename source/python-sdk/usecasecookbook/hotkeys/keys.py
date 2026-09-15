"""Mirrors ../../../java-sdk's hotkeys/HotKeyKeys.java.

Key helpers for the hot-key demonstrations. The logical product id is ``product_id``;
replica keys append ``:index`` so traffic can be spread across multiple records.
"""

import random
from typing import List

from aerospike_async import Key
from aerospike_sdk import DataSet

from usecasecookbook import config

PRODUCTS = DataSet.of(config.NAMESPACE, "uccb_hotkey")


def primary(product_id: int) -> Key:
    return PRODUCTS.id(product_id)


def replica(product_id: int, index: int) -> Key:
    return PRODUCTS.id(f"{product_id}:{index}")


def random_replica(product_id: int, replica_count: int) -> Key:
    index = random.randrange(replica_count)
    return replica(product_id, index)


def all_replica_keys(product_id: int, replica_count: int) -> List[Key]:
    return [replica(product_id, i) for i in range(replica_count)]


def primary_and_all_replica_keys(product_id: int, replica_count: int) -> List[Key]:
    return [primary(product_id)] + all_replica_keys(product_id, replica_count)
