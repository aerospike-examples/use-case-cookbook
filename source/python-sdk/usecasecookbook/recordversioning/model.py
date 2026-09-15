"""Mirrors ../../../java-sdk's recordversioning/model/{TradeBase,TradeStaticData}.java.

No annotation-driven object mapper exists in this SDK, so both models are plain
dataclasses with hand-written ``to_bins``/``from_bins`` - see setup/model.py for the
established pattern.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class TradeBase:
    id: int
    source_system_id: str = ""
    version: int = 0
    parent_trade_id: int = 0
    ext_trade_id: str = ""
    content_id: int = 0
    book: str = ""
    counterparty: str = ""
    trade_date: float = 0.0
    entered_date: float = 0.0
    updated_date: float = 0.0
    trade_version: int = 0
    record_complete: bool = True
    data_version: int = 0
    versions: dict[int, int] = field(default_factory=dict)

    def to_bins(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "sourceSystemId": self.source_system_id,
            "version": self.version,
            "parentTradeId": self.parent_trade_id,
            "extTradeId": self.ext_trade_id,
            "contentId": self.content_id,
            "book": self.book,
            "counterparty": self.counterparty,
            "tradeDate": self.trade_date,
            "enteredDate": self.entered_date,
            "updatedDate": self.updated_date,
            "tradeVersion": self.trade_version,
            "recordComplete": self.record_complete,
            "dataVersion": self.data_version,
            "versions": self.versions,
        }

    @staticmethod
    def from_bins(bins: dict[str, Any]) -> "TradeBase":
        return TradeBase(
            id=bins["id"],
            source_system_id=bins.get("sourceSystemId", ""),
            version=bins.get("version", 0),
            parent_trade_id=bins.get("parentTradeId", 0),
            ext_trade_id=bins.get("extTradeId", ""),
            content_id=bins.get("contentId", 0),
            book=bins.get("book", ""),
            counterparty=bins.get("counterparty", ""),
            trade_date=bins.get("tradeDate", 0.0),
            entered_date=bins.get("enteredDate", 0.0),
            updated_date=bins.get("updatedDate", 0.0),
            trade_version=bins.get("tradeVersion", 0),
            record_complete=bins.get("recordComplete", True),
            data_version=bins.get("dataVersion", 0),
            versions=bins.get("versions") or {},
        )


@dataclass
class TradeStaticData:
    trade_id: int
    version: int = 0
    data: str | None = None
    mutable_data: int = 0

    def to_bins(self) -> dict[str, Any]:
        return {
            "tradeId": self.trade_id,
            "version": self.version,
            "data": self.data,
            "mutableData": self.mutable_data,
        }

    @staticmethod
    def from_bins(bins: dict[str, Any]) -> "TradeStaticData":
        return TradeStaticData(
            trade_id=bins["tradeId"],
            version=bins.get("version", 0),
            data=bins.get("data"),
            mutable_data=bins.get("mutableData", 0),
        )
