"""Port of the legacy TimeSeriesDemo (see ../../java and ../../java-sdk). Stores IoT device
events partitioned by account and bucketed into fixed-width time windows (one record per account
per bucket); each record holds a key-ordered map from a time-sortable ``eventId`` to a
``[deviceId, eventDetails]`` pair, so range/pagination queries against a bucket become map
key-range operations.

Device filtering: the java-sdk port pushes device filtering down server-side with a single AEL
expression combining a map key-range selector with a ``&[?(...)]`` filter-chain clause (canonical
AEL grammar §4.4 - a standard construct, not Java-SDK-specific, despite an earlier version of
this comment claiming otherwise). The currently-published ``aerospike-sdk==0.9.0a5`` package
can't run it, though: that package parses AEL strings with its own bundled, client-side grammar
(``aerospike_sdk/ael/antlr4/Condition.g4``) rather than sending them to the server for
compilation, and that bundled grammar predates the filter-chain construct entirely - confirmed
empirically against a live cluster:

    session.query(key).bin("map").select_from(
        "$.map.{lo-hi}&[?(@.[0] in ['dev1','dev2'])]"
    ).execute()
    -> AelParseException: line 1:17 token recognition error at: '?('

The SDK's actively developed (but not yet publicly released) branch has since moved AEL
compilation server-side and supports this construct - this is the same stale-local-grammar
situation as the type-suffix gap documented in ../advancedexpressions/advanced_expressions.py,
not a permanent limitation of the language.

So this port fetches the eventId range server-side via the native ``on_map_key_range(...)
.get_values()`` CDT operation (no AEL needed for the no-device-filter case - and actually a
cleaner, more direct read than an AEL round-trip), then applies the device-id filter client-side
in Python.
"""

import random
import uuid
from collections.abc import Sequence
from datetime import datetime, timezone

from aerospike_async import MapOrder, ResultCode
from aerospike_sdk import DataSet, SyncSession
from aerospike_sdk.exceptions import AerospikeError

from usecasecookbook import config
from usecasecookbook.ansi_colors import RESET, YELLOW
from usecasecookbook.timeseries.model import Account, Event
from usecasecookbook.use_case import UseCase

BIN_NAME = "map"

_MILLIS_PER_HOUR = 3_600_000
_MILLIS_PER_DAY = 24 * _MILLIS_PER_HOUR
_DATE_OFFSET_MILLIS = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
_BUCKET_WIDTH_HOURS = 24
_MAX_DAYS_TO_STORE = 14
_HOURS_PER_DAY = 24
_EVENT_ID_TIMESTAMP_LENGTH = 13

_NUM_ACCOUNTS = 10
_MAX_EVENTS_PER_DEVICE = 800
_DEFAULT_VIDEO_URL = "https://somewhere.com/4659278373492"
_DEFAULT_STORAGE_LOCATION = "hv"

EVENTS = DataSet.of(config.NAMESPACE, "uccb_events")

ASCENDING = "ASCENDING"
DESCENDING = "DESCENDING"

_expiration_warning_shown = False


# ----------------------------------------------------------------------
# Bucket / event-id helpers
# ----------------------------------------------------------------------

def _bucket_offset(timestamp_ms: int) -> int:
    """The bucket offset from the reference date for a given timestamp."""
    return (timestamp_ms - _DATE_OFFSET_MILLIS) // (_MILLIS_PER_HOUR * _BUCKET_WIDTH_HOURS)


def _event_key(account_id: str, timestamp_ms: int):
    return EVENTS.id(f"{account_id}:{_bucket_offset(timestamp_ms)}")


def _extract_timestamp_from_event_id(event_id: str) -> int:
    return int(event_id[:_EVENT_ID_TIMESTAMP_LENGTH])


def _event_id_from_timestamp(timestamp_ms: int, lower_bound: bool) -> str:
    """The lowest (``lower_bound``) or highest possible eventId for that timestamp."""
    if lower_bound:
        return f"{timestamp_ms:013d}{0:012d}"
    return f"{timestamp_ms:013d}{999999999999}"


def _next_event_id(event_id: str) -> str:
    value = int(event_id[_EVENT_ID_TIMESTAMP_LENGTH:])
    return f"{event_id[:_EVENT_ID_TIMESTAMP_LENGTH]}{value + 1:012d}"


# ----------------------------------------------------------------------
# Writes
# ----------------------------------------------------------------------

def upsert_event(session: SyncSession, event: Event, set_expiry: bool) -> None:
    """Insert or update an event. If ``set_expiry`` is true, the bucket record's TTL is set to
    the retention window; otherwise its TTL is left unchanged.
    """
    _validate_event(event)
    event_time_ms = int(event.timestamp.timestamp() * 1000)
    key = _event_key(event.account_id, event_time_ms)
    _write_event(session, key, event, set_expiry)


def _write_event(session: SyncSession, key, event: Event, set_expiry: bool) -> None:
    global _expiration_warning_shown

    op = session.upsert(key)
    op = (
        op.expire_record_after_seconds(_MAX_DAYS_TO_STORE * 86400)
        if set_expiry
        else op.with_no_change_in_expiration()
    )
    try:
        (
            op.bin(BIN_NAME)
            .on_map_key(event.id, create_type=MapOrder.KEY_ORDERED)
            .set_to([event.device_id, event.to_map()])
            .execute()
        )
    except AerospikeError as ae:
        # Some namespaces (eviction/nsup disabled, e.g. this dev cluster's "test" namespace)
        # reject an explicit record TTL with FAIL_FORBIDDEN. Fall back to writing without one.
        if set_expiry and ae.result_code == ResultCode.FAIL_FORBIDDEN:
            if not _expiration_warning_shown:
                _expiration_warning_shown = True
                print(
                    f"{YELLOW}Note: this namespace does not support record expiration "
                    f"(eviction is disabled) - events will be written without a TTL.{RESET}"
                )
            _write_event(session, key, event, False)
        else:
            raise


# ----------------------------------------------------------------------
# Reads
# ----------------------------------------------------------------------

def _oldest_timestamp(start_timestamp: int | None) -> int:
    if start_timestamp is not None:
        return start_timestamp
    return int(datetime.now(timezone.utc).timestamp() * 1000) - _MAX_DAYS_TO_STORE * _MILLIS_PER_DAY


def _latest_timestamp(end_timestamp: int | None) -> int:
    if end_timestamp is not None:
        return end_timestamp
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def get_events_between(
    session: SyncSession,
    account_id: str,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
    event_id: str | None = None,
    count: int = 50,
    direction: str = DESCENDING,
    device_ids: Sequence[str] = (),
) -> list[Event]:
    """Retrieve events for an account between the given date range, starting with the newest
    (or oldest, per ``direction``). If ``event_id`` is passed, results are exclusive of it,
    allowing this to be used for pagination.

    ``event_id`` is the event ID to page from (``None`` for most recent). With
    ``DESCENDING`` this is the exclusive upper bound (only older events are returned); with
    ``ASCENDING`` it's the exclusive lower bound (only newer events are returned) - i.e. it's
    always the last event ID seen on the previous page.
    """
    _validate_account_id(account_id)
    _validate_count(count)
    _validate_timestamps(start_timestamp, end_timestamp)

    results: list[Event] = []

    if event_id is not None:
        if direction == ASCENDING:
            earliest_event_id = _next_event_id(event_id)
            latest_event_id = _event_id_from_timestamp(_latest_timestamp(end_timestamp), False)
        else:
            latest_event_id = event_id
            earliest_event_id = _event_id_from_timestamp(_oldest_timestamp(start_timestamp), True)
    else:
        latest_event_id = _event_id_from_timestamp(_latest_timestamp(end_timestamp), False)
        earliest_event_id = _event_id_from_timestamp(_oldest_timestamp(start_timestamp), True)

    start_record = _bucket_offset(_extract_timestamp_from_event_id(earliest_event_id))
    end_record = _bucket_offset(_extract_timestamp_from_event_id(latest_event_id))

    device_filter: set[str] | None = set(device_ids) if device_ids else None

    if direction == ASCENDING:
        record_key = start_record
        while len(results) < count and record_key <= end_record:
            entries = _read_filtered_bucket(
                session, EVENTS.id(f"{account_id}:{record_key}"),
                earliest_event_id, latest_event_id, device_filter,
            )
            _add_events_to_results(count, entries, results, direction)
            record_key += 1
    else:
        record_key = end_record
        while len(results) < count and record_key >= start_record:
            entries = _read_filtered_bucket(
                session, EVENTS.id(f"{account_id}:{record_key}"),
                earliest_event_id, latest_event_id, device_filter,
            )
            _add_events_to_results(count, entries, results, direction)
            record_key -= 1

    return results


def get_events_before(
    session: SyncSession, account_id: str, event_id: str | None = None,
    count: int = 50, device_ids: Sequence[str] = (),
) -> list[Event]:
    """Retrieve events for an account before a specified event ID (newest first)."""
    return get_events_between(session, account_id, None, None, event_id, count, DESCENDING, device_ids)


def get_events_after(
    session: SyncSession, account_id: str, event_id: str,
    count: int = 50, device_ids: Sequence[str] = (),
) -> list[Event]:
    """Retrieve events for an account after a specified event ID (oldest first)."""
    return get_events_between(session, account_id, None, None, event_id, count, ASCENDING, device_ids)


def get_total_events_for_account(session: SyncSession, account_id: str) -> int:
    """The total number of events currently stored for an account (sums ``map_size()`` across
    every bucket in the retention window, via one multi-key batch read)."""
    _validate_account_id(account_id)

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    first_record = _bucket_offset(now_ms)
    buckets_for_time_range = (_MAX_DAYS_TO_STORE * _HOURS_PER_DAY + _BUCKET_WIDTH_HOURS - 1) // _BUCKET_WIDTH_HOURS
    end_record = first_record - buckets_for_time_range

    keys = [EVENTS.id(f"{account_id}:{i}") for i in range(end_record, first_record + 1)]

    total = 0
    stream = session.query(keys).bin(BIN_NAME).map_size().execute()
    for row in stream:
        if row.is_ok and row.record is not None:
            total += row.record.bins.get(BIN_NAME, 0)
    stream.close()
    return total


def _read_filtered_bucket(
    session: SyncSession, key, earliest_event_id: str, latest_event_id: str,
    device_filter: set[str] | None,
) -> list:
    """Read a bucket's events restricted to an eventId key range via the native
    ``on_map_key_range(...).get_values()`` CDT operation (server-side, key-ordered). Device
    filtering (when requested) happens client-side afterwards - see the module docstring for why
    it isn't pushed down server-side here.
    """
    stream = (
        session.query(key).bin(BIN_NAME)
        .on_map_key_range(earliest_event_id, latest_event_id)
        .get_values()
        .execute()
    )
    row = stream.first()
    stream.close()
    if row is None or row.record is None:
        return []
    entries = row.record.bins.get(BIN_NAME) or []
    if device_filter:
        entries = [entry for entry in entries if entry[0] in device_filter]
    return entries


def _add_events_to_results(count: int, entries: list, results: list[Event], direction: str) -> None:
    if not entries:
        return
    ordered = reversed(entries) if direction == DESCENDING else entries
    for device_id, event_map in ordered:
        if len(results) >= count:
            break
        results.append(Event.from_map(event_map))


# ----------------------------------------------------------------------
# Validation
# ----------------------------------------------------------------------

def _validate_event(event: Event) -> None:
    if event is None:
        raise ValueError("Event cannot be null")
    if not event.account_id or not event.account_id.strip():
        raise ValueError("Event account ID cannot be null or empty")
    if event.timestamp is None:
        raise ValueError("Event timestamp cannot be null")


def _validate_account_id(account_id: str) -> None:
    if not account_id or not account_id.strip():
        raise ValueError("Account ID cannot be null or empty")


def _validate_count(count: int) -> None:
    if count <= 0:
        raise ValueError("Count must be positive")


def _validate_timestamps(start_timestamp: int | None, end_timestamp: int | None) -> None:
    if start_timestamp is not None and end_timestamp is not None and start_timestamp >= end_timestamp:
        raise ValueError("start_timestamp must be less than end timestamp")


# ----------------------------------------------------------------------
# Data generation
# ----------------------------------------------------------------------

def generate_sample_event(account_id: str, device_id: str) -> Event:
    fourteen_days_ms = _MAX_DAYS_TO_STORE * _MILLIS_PER_DAY
    timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - random.randrange(fourteen_days_ms)
    random_value = random.randrange(999999999999)
    return _build_event(account_id, device_id, timestamp_ms, random_value)


def _build_event(account_id: str, device_id: str, timestamp_ms: int, random_value: int) -> Event:
    return Event(
        id=f"{timestamp_ms:013d}{random_value:012d}",
        account_id=account_id,
        device_id=device_id,
        timestamp=datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc),
        resolution=[1920, 1080],
        parameter_tags=[f"tag-{random.randrange(5)}"],
        partner_id=str(uuid.uuid4()),
        partner_state_id=f"state {random.randrange(1000)}",
        video_meta={"duration": 13, "videoUrl": _DEFAULT_VIDEO_URL},
        parameters={
            "imageMeta": {"assetId": "", "frameIndex": 0, "storageLocation": _DEFAULT_STORAGE_LOCATION},
            "imageUrl": "",
            "objectsDetected": [
                {"frameIndex": 0, "type": "person"},
                {"frameIndex": 0, "type": "motion"},
            ],
        },
    )


def display_events(events: list[Event]) -> None:
    for i, event in enumerate(events, start=1):
        print(f"{i:2d}: {event.id} - {event.timestamp} - {event.device_id}")


def _generate_sample_data(session: SyncSession) -> None:
    devices_created = 0
    events_created = 0

    for account_num in range(1, _NUM_ACCOUNTS + 1):
        account = Account(f"acct-{account_num}", random.randint(1, 20))
        if account.id == "acct-1":
            account.num_devices = max(10, account.num_devices)

        for device_num in range(account.num_devices):
            events_this_device = random.randrange(_MAX_EVENTS_PER_DEVICE)
            for _ in range(events_this_device):
                event = generate_sample_event(account.id, f"device-{account.id}-{device_num}")
                upsert_event(session, event, True)
                events_created += 1
            devices_created += 1
        print(f"{account_num:,} accounts, {devices_created:,} devices, {events_created:,} events")


def _demonstrate_queries(session: SyncSession) -> None:
    print(f"Account acct-1 has {get_total_events_for_account(session, 'acct-1'):,} events\n")

    print("First list -- acct-1, all devices")
    events = get_events_before(session, "acct-1", None, 50)
    display_events(events)

    events = get_events_before(session, "acct-1", events[-1].id, 50)
    print("\nSecond page:")
    display_events(events)

    print("First list -- acct-1, devices 1, 2, 3")
    page_size = 25
    events = get_events_before(
        session, "acct-1", None, page_size,
        ["device-acct-1-1", "device-acct-1-2", "device-acct-1-3"],
    )
    display_events(events)

    page_counter = 1
    event_id_at_top_of_page = None
    while len(events) == page_size:
        page_counter += 1
        print(f"Page {page_counter:,}")
        event_id_at_top_of_page = events[-1].id
        events = get_events_before(
            session, "acct-1", event_id_at_top_of_page, page_size,
            ["device-acct-1-1", "device-acct-1-2", "device-acct-1-3"],
        )
        display_events(events)

    print(f"\nGetting NEXT (ascending) 35 events after {event_id_at_top_of_page}")
    events = get_events_after(
        session, "acct-1", event_id_at_top_of_page, 35,
        ["device-acct-1-1", "device-acct-1-2", "device-acct-1-3"],
    )
    display_events(events)

    end_time = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_time = end_time - 2 * _MILLIS_PER_DAY
    print("\nShowing last 2 days of events in descending order")
    events = get_events_between(
        session, "acct-1", start_time, end_time, None, 100_000, DESCENDING,
        ["device-acct-1-8", "device-acct-1-9", "device-acct-10"],
    )
    display_events(events)

    print("\nShowing last 2 days of events in ascending order")
    events = get_events_between(
        session, "acct-1", start_time, end_time, None, 100_000, ASCENDING,
        ["device-acct-1-8", "device-acct-1-9", "device-acct-10"],
    )
    display_events(events)


class TimeSeriesDemo(UseCase):
    def get_name(self) -> str:
        return "Predictable time-series data"

    def get_description(self) -> str:
        return (
            "Demonstrates how to store, update and query time-series data. In this case the data is "
            "devices which generate events. These devices could be motion sensors, cameras, etc. "
            "The data model has many accounts, each account has a handful of devices, and the devices "
            "generate events when triggered. The events are stored for 14 days, and queries can be "
            "performed on the events for an account, filtering by time range and / or a list of device ids. "
            "This shows a way to store time series data with events occurring on a sporadic (random) basis, with "
            "low variation in cardinality, or events occurring on a periodic basis like stock ticks."
        )

    def get_reference(self) -> str:
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/timeseries.md"

    def setup(self, session: SyncSession) -> None:
        session.truncate(EVENTS)
        _generate_sample_data(session)

    def run(self, session: SyncSession) -> None:
        _demonstrate_queries(session)
