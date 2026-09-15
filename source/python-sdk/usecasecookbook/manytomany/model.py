"""Mirrors ../../../java-sdk's manytomany/model/{Account,Customer}.java.

No object mapper exists in this SDK - each model is a plain dataclass with hand-written
``to_bins``/``from_bins`` (see ../setup/model.py for the established pattern).
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict


@dataclass
class Account:
    id: str
    account_name: str
    balance_in_cents: int
    date_opened: datetime

    def to_bins(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "accountName": self.account_name,
            "balanceInCents": self.balance_in_cents,
            "dateOpened": self.date_opened.timestamp(),
        }

    @staticmethod
    def from_bins(bins: Dict[str, Any]) -> "Account":
        return Account(
            id=bins["id"],
            account_name=bins["accountName"],
            balance_in_cents=bins["balanceInCents"],
            date_opened=datetime.fromtimestamp(bins["dateOpened"]),
        )


@dataclass
class Customer:
    cust_id: str
    first_name: str
    last_name: str
    dob: datetime
    date_joined: datetime

    def to_bins(self) -> Dict[str, Any]:
        return {
            "custId": self.cust_id,
            "firstName": self.first_name,
            "lastName": self.last_name,
            "dob": self.dob.timestamp(),
            "dateJoined": self.date_joined.timestamp(),
        }

    @staticmethod
    def from_bins(bins: Dict[str, Any]) -> "Customer":
        return Customer(
            cust_id=bins["custId"],
            first_name=bins["firstName"],
            last_name=bins["lastName"],
            dob=datetime.fromtimestamp(bins["dob"]),
            date_joined=datetime.fromtimestamp(bins["dateJoined"]),
        )
