"""Mirrors ../../../java-sdk's manytomany/model/{Account,Customer}.java.

No object mapper exists in this SDK - each model is a plain dataclass with hand-written
``to_bins``/``from_bins`` (see ../setup/model.py for the established pattern).
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


@dataclass
class Customer:
    cust_id: str
    first_name: str
    last_name: str
    dob: datetime
    date_joined: datetime

    def to_bins(self) -> dict[str, Any]:
        return {
            "custId": self.cust_id,
            "firstName": self.first_name,
            "lastName": self.last_name,
            "dob": self.dob.timestamp(),
            "dateJoined": self.date_joined.timestamp(),
        }

    @staticmethod
    def from_bins(bins: dict[str, Any]) -> "Customer":
        return Customer(
            cust_id=bins["custId"],
            first_name=bins["firstName"],
            last_name=bins["lastName"],
            dob=datetime.fromtimestamp(bins["dob"], tz=timezone.utc),
            date_joined=datetime.fromtimestamp(bins["dateJoined"], tz=timezone.utc),
        )
