"""Mirrors ../../../java-sdk's setup/model/Account.java.

This SDK has no annotation-driven object mapper (unlike aerospike-sdk-mapper-java on the
Java SDK side), so each model is a plain dataclass with its own ``to_bins``/``from_bins``
to convert to/from the dict a record's bins are represented as.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass
class Account:
    id: str
    account_name: str
    balance_in_cents: int
    date_opened: datetime

    def to_bins(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "accountName": self.account_name,
            "balanceInCents": self.balance_in_cents,
            "dateOpened": self.date_opened.timestamp(),
        }

    @staticmethod
    def from_bins(bins: dict[str, Any]) -> "Account":
        return Account(
            id=bins["id"],
            account_name=bins["accountName"],
            balance_in_cents=bins["balanceInCents"],
            date_opened=datetime.fromtimestamp(bins["dateOpened"], tz=timezone.utc),
        )
