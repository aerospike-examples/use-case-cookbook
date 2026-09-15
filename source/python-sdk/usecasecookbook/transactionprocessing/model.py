"""Mirrors ../../../java-sdk's transactionprocessing/model/{Account,Transaction}.java.

The account's recent-transactions maps (``txns_dc1``/``txns_dc2``) are managed as raw CDT
bins directly (see top_transactions_across_dcs.py), not modeled as dataclass fields - only
``id`` is needed on the Account side.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class Account:
    id: str

    def to_bins(self) -> Dict[str, Any]:
        return {"id": self.id}

    @staticmethod
    def from_bins(bins: Dict[str, Any]) -> "Account":
        return Account(id=bins["id"])


@dataclass
class Transaction:
    id: str
    timestamp: int
    amount: int
    desc: str
    status: str
    origin: Optional[str]
    approval_code: str
    account_id: str

    def to_bins(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "amount": self.amount,
            "desc": self.desc,
            "status": self.status,
            "origin": self.origin,
            "approvalCode": self.approval_code,
            "accountId": self.account_id,
        }

    @staticmethod
    def from_bins(bins: Dict[str, Any]) -> "Transaction":
        return Transaction(
            id=bins["id"],
            timestamp=bins["timestamp"],
            amount=bins["amount"],
            desc=bins["desc"],
            status=bins["status"],
            origin=bins.get("origin"),
            approval_code=bins["approvalCode"],
            account_id=bins["accountId"],
        )
