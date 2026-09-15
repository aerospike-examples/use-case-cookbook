"""Mirrors ../../../java-sdk's timeseries/model/{Account,Event}.java.

Events live nested inside a bucket record's map bin (one map entry per event, keyed by a
time-sortable ``eventId``), never as their own top-level record - see time_series_demo.py /
time_series_large_variance_demo.py. So ``Event`` gets ``to_map``/``from_map`` (a nested dict
value, not a whole record's bins) rather than the ``to_bins``/``from_bins`` convention used by
dataclass-mapped records elsewhere in this cookbook (see setup/model.py).
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class Account:
    """A purely in-memory concept used to drive sample-data generation - accounts are never
    persisted as their own Aerospike record in this use case, only their events are (see
    Event)."""

    id: str
    num_devices: int


@dataclass
class Event:
    # id is always 25 characters: the first 13 are a timestamp, the rest make it unique
    id: str
    account_id: str
    device_id: str
    timestamp: Optional[datetime]
    parameters: Optional[Dict[str, Any]] = None
    resolution: Optional[List[int]] = None
    video_meta: Optional[Dict[str, Any]] = None
    parameter_tags: Optional[List[str]] = None
    partner_id: Optional[str] = None
    partner_state_id: Optional[str] = None

    def to_map(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "accountId": self.account_id,
            "deviceId": self.device_id,
            "params": self.parameters,
            "resolution": self.resolution,
            "videoMeta": self.video_meta,
            "paramTags": self.parameter_tags,
            "partnerId": self.partner_id,
            "partStateId": self.partner_state_id,
            "timestamp": int(self.timestamp.timestamp() * 1000) if self.timestamp else 0,
        }

    @staticmethod
    def from_map(event_map: Dict[str, Any]) -> "Event":
        timestamp_ms = event_map.get("timestamp")
        return Event(
            id=event_map.get("id"),
            account_id=event_map.get("accountId"),
            device_id=event_map.get("deviceId"),
            parameters=event_map.get("params"),
            resolution=event_map.get("resolution"),
            video_meta=event_map.get("videoMeta"),
            parameter_tags=event_map.get("paramTags"),
            partner_id=event_map.get("partnerId"),
            partner_state_id=event_map.get("partStateId"),
            timestamp=(
                datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                if timestamp_ms
                else None
            ),
        )
