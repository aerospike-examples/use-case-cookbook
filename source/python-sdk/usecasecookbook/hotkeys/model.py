"""Mirrors ../../../java-sdk's hotkeys/model/HotKeyProduct.java.

Only the primary (single-key) record maps onto this class - replica keys use a custom
``productId:index`` id (see keys.py), which can't be derived from a single ``id`` field, so
those are still written as raw bins (see product_setup.py).
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class HotKeyProduct:
    id: int
    sku: str
    description: str
    units_sold: int

    def to_bins(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "sku": self.sku,
            "description": self.description,
            "unitsSold": self.units_sold,
        }

    @staticmethod
    def from_bins(bins: dict[str, Any]) -> "HotKeyProduct":
        return HotKeyProduct(
            id=bins["id"], sku=bins["sku"], description=bins["description"],
            units_sold=bins["unitsSold"],
        )
