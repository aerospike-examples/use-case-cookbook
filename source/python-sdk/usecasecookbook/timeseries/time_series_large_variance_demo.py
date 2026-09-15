"""Port of the legacy TimeSeriesLargeVarianceDemo (see ../../java and ../../java-sdk). Same
bucketed time-series model as time_series_demo.py, but each bucket record adaptively splits once
it holds more than ``MAX_RECORDS_PER_BUCKET`` events: the oldest ``PERCENT_EVENTS_IN_ORIG_BUCKET``
percent stay in the original ("root") record, and the rest move to a new continuation sub-record
named after the lowest eventId it holds. The root record's ``cont`` bin holds an ascending list of
these split-point eventIds, so a query walks that list to find which sub-record(s) it needs, and a
write walks it to find which sub-record a new event belongs in (recursing into that sub-record's
own split if it, in turn, overflows).

``upsert_event`` tries ``_try_write_to_root_block`` first: a single non-transactional, filtered
``upsert().where(...)`` call that succeeds without ever opening a transaction as long as the root
block hasn't split and has room - the common case. Only when that filter blocks the write (root
already split, or this write would overflow it) does ``upsert_event`` fall back to
``run_in_transaction`` to find or create the right target block.

``_split_bucket`` detects overflow and removes the minority slice in one call against the bucket
record, gated by ``where()`` so the whole operate() is a no-op if another writer already handled
the split (or the bucket has since shrunk back under the threshold). The java-sdk port does this
as a single ``when(...)`` AEL expression mixing a conditional read (``select_from``) and a
conditional write (``upsert_from``) of the same condition in one round trip. Attempting the direct
equivalent here - a ``when(count >= N => ....remove(), default => ....get(return: UNORDERED_MAP))``
expression written via ``upsert_from`` - fails server-side:

    session.upsert(key).bin("map").upsert_from(
        "when ($.map.{}.count() >= 10 => $.map.{-2:}.remove(), "
        "default => $.map.{0:}.get(return: UNORDERED_MAP))"
    ).execute()
    -> AerospikeError: Code: ParameterError, ...

(mixing a mutating path function - ``remove()`` - with a value-producing one - ``get(...)`` - as
alternate ``when()`` branches is rejected server-side; each branch of a ``when()`` must be a pure
value expression). So this port instead uses the ``where()`` clause on the whole write segment as
the overflow gate, and issues the minority read (cross-bin, so it still needs an AEL
``select_from``) and the majority removal (same-bin, so a native CDT ``on_map_index_range(...)
.remove()`` op) as two ops in that one gated call - still one atomic round trip, just built from
two simpler pieces instead of one `when()` expression.

The split point (``MINOR_SPLIT_ITEMS``) is a fixed item count rather than a percentage of the
bucket's current size: like the java-sdk port, a computed bound (e.g. ``count() * 80 / 100``)
isn't expressible - the map/list range selectors in this SDK's AEL grammar
(``aerospike_sdk/ael/antlr4/Condition.g4``, ``indexRangeIdentifier: start ':' end``, where
``start``/``end`` are ``signedInt`` - i.e. integer literal tokens) require static literal bounds.
"""

import random
import uuid
from collections.abc import Sequence
from datetime import datetime, timezone

from aerospike_async import ListOrderType, MapOrder, MapReturnType, ResultCode
from aerospike_sdk import DataSet, SyncSession
from aerospike_sdk.exceptions import AerospikeError

from usecasecookbook import config
from usecasecookbook.ansi_colors import RESET, YELLOW
from usecasecookbook.timeseries.model import Account, Event
from usecasecookbook.txn import run_in_transaction
from usecasecookbook.use_case import UseCase

BIN_NAME = "map"
CONTINUATION_BIN = "cont"
MAX_RECORDS_PER_BUCKET = 10
PERCENT_EVENTS_IN_ORIG_BUCKET = 80
MINOR_SPLIT_ITEMS = MAX_RECORDS_PER_BUCKET * (100 - PERCENT_EVENTS_IN_ORIG_BUCKET) // 100

_MILLIS_PER_HOUR = 3_600_000
_MILLIS_PER_DAY = 24 * _MILLIS_PER_HOUR
_DATE_OFFSET_MILLIS = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
_BUCKET_WIDTH_HOURS = 24
_MAX_DAYS_TO_STORE = 14
_EVENT_ID_TIMESTAMP_LENGTH = 13

_NUM_ACCOUNTS = 100
_MAX_EVENTS_PER_DEVICE = 20
_NUM_EVENTS_ACCT_1 = 25_000
_DEFAULT_VIDEO_URL = "https://somewhere.com/4659278373492"
_DEFAULT_STORAGE_LOCATION = "hv"

EVENTS = DataSet.of(config.NAMESPACE, "uccb_events_variance")

ASCENDING = "ASCENDING"
DESCENDING = "DESCENDING"

_expiration_warning_shown = False


# ----------------------------------------------------------------------
# Bucket / event-id helpers (identical logic to time_series_demo.py)
# ----------------------------------------------------------------------

def _bucket_offset(timestamp_ms: int) -> int:
    return (timestamp_ms - _DATE_OFFSET_MILLIS) // (_MILLIS_PER_HOUR * _BUCKET_WIDTH_HOURS)


def _root_key(account_id: str, timestamp_ms: int):
    return EVENTS.id(f"{account_id}:{_bucket_offset(timestamp_ms)}")


def _continuation_key(root_key, sub_key: str):
    return EVENTS.id(f"{root_key.value}-{sub_key}")


def _extract_timestamp_from_event_id(event_id: str) -> int:
    return int(event_id[:_EVENT_ID_TIMESTAMP_LENGTH])


def _event_id_from_timestamp(timestamp_ms: int, lower_bound: bool) -> str:
    if lower_bound:
        return f"{timestamp_ms:013d}{0:012d}"
    return f"{timestamp_ms:013d}{999999999999}"


def _next_event_id(event_id: str) -> str:
    value = int(event_id[_EVENT_ID_TIMESTAMP_LENGTH:])
    return f"{event_id[:_EVENT_ID_TIMESTAMP_LENGTH]}{value + 1:012d}"


# ----------------------------------------------------------------------
# Writes: insert with adaptive bucket splitting
# ----------------------------------------------------------------------

def upsert_event(session: SyncSession, event: Event, set_expiry: bool) -> None:
    _validate_event(event)
    root_key = _root_key(event.account_id, int(event.timestamp.timestamp() * 1000))

    if _try_write_to_root_block(session, root_key, event, set_expiry):
        return

    # The fast path above was filtered out - the root block has either already split, or this
    # write would overflow it - so a transaction (with its extra reads) is genuinely needed to
    # find or create the right target block. Every other write short-circuits before ever
    # opening one.
    def _find_and_write(tx: SyncSession) -> None:
        continuation = _read_continuation_list(tx, root_key)
        target_key = root_key
        for sub_key in reversed(continuation):
            if sub_key <= event.id:
                target_key = _continuation_key(root_key, sub_key)
                break

        size = _write_event_and_get_bucket_size(tx, target_key, event, set_expiry)
        if size > MAX_RECORDS_PER_BUCKET:
            _split_bucket(tx, root_key, target_key)

    run_in_transaction(session, _find_and_write)


def _try_write_to_root_block(session: SyncSession, root_key, event: Event, set_expiry: bool) -> bool:
    """The happy path: a single non-transactional ``upsert().where(...)`` call that inserts the
    event into the root block, filtered to only apply when the root hasn't split yet and still
    has room. Returns ``False`` - without writing anything - when the filter blocks the write, so
    the caller can fall back to the slower path that finds (or creates) the correct target block.
    """
    global _expiration_warning_shown

    can_write_to_root_ael = (
        f"$.{CONTINUATION_BIN}.exists() == false and $.{BIN_NAME}.{{}}.count() < {MAX_RECORDS_PER_BUCKET}"
    )
    op = session.upsert(root_key).where(can_write_to_root_ael)
    op = (
        op.expire_record_after_seconds(_MAX_DAYS_TO_STORE * 86400)
        if set_expiry
        else op.with_no_change_in_expiration()
    )
    try:
        result = (
            op.bin(BIN_NAME)
            .on_map_key(event.id, create_type=MapOrder.KEY_ORDERED)
            .set_to([event.device_id, event.to_map()])
            .execute()
            .first()
        )
        return result is not None
    except AerospikeError as ae:
        # Same eviction-disabled namespace gotcha as time_series_demo.py - see there for details.
        if set_expiry and ae.result_code == ResultCode.FAIL_FORBIDDEN:
            if not _expiration_warning_shown:
                _expiration_warning_shown = True
                print(
                    f"{YELLOW}Note: this namespace does not support record expiration "
                    f"(eviction is disabled) - events will be written without a TTL.{RESET}"
                )
            return _try_write_to_root_block(session, root_key, event, False)
        raise


def _read_continuation_list(session: SyncSession, root_key) -> list[str]:
    row = session.query(root_key).bins([CONTINUATION_BIN]).execute().first()
    if row is None or row.record is None:
        return []
    return list(row.record.bins.get(CONTINUATION_BIN) or [])


def _write_event_and_get_bucket_size(session: SyncSession, key, event: Event, set_expiry: bool) -> int:
    global _expiration_warning_shown

    op = session.upsert(key)
    op = (
        op.expire_record_after_seconds(_MAX_DAYS_TO_STORE * 86400)
        if set_expiry
        else op.with_no_change_in_expiration()
    )
    try:
        # The map put operation's own result is the bucket's new size - no separate read needed.
        row = (
            op.bin(BIN_NAME)
            .on_map_key(event.id, create_type=MapOrder.KEY_ORDERED)
            .set_to([event.device_id, event.to_map()])
            .execute()
            .first()
        )
        return row.record.bins[BIN_NAME]
    except AerospikeError as ae:
        if set_expiry and ae.result_code == ResultCode.FAIL_FORBIDDEN:
            if not _expiration_warning_shown:
                _expiration_warning_shown = True
                print(
                    f"{YELLOW}Note: this namespace does not support record expiration "
                    f"(eviction is disabled) - events will be written without a TTL.{RESET}"
                )
            return _write_event_and_get_bucket_size(session, key, event, False)
        raise


def _split_bucket(session: SyncSession, root_key, bucket_key) -> None:
    """Split an overflowing bucket: read its oldest ``MINOR_SPLIT_ITEMS`` events and remove them
    from the bucket in one gated call (see the module docstring for why this isn't a single
    ``when(...)`` expression), then - if anything was removed - persist that minority slice to a
    new continuation sub-record and record the split point on the root record's (ascending,
    ordered) ``cont`` list.

    The ``where(...)`` guard re-checks the size rather than trusting the caller, because
    ``upsert_event`` calls this right after a separate write that may have raced with another
    writer (under the non-transactional fallback - see usecasecookbook/txn.py).
    """
    minority_ael = f"$.{BIN_NAME}.{{-{MINOR_SPLIT_ITEMS}:}}.get(return: UNORDERED_MAP)"

    row = (
        session.upsert(bucket_key)
        .where(f"$.{BIN_NAME}.{{}}.count() >= {MAX_RECORDS_PER_BUCKET}")
        .bin("minorityOut").select_from(minority_ael)
        .bin(BIN_NAME).on_map_index_range(-MINOR_SPLIT_ITEMS).remove(return_type=MapReturnType.COUNT)
        .execute()
        .first()
    )
    if row is None or row.record is None:
        return

    minority_map = row.record.bins.get("minorityOut")
    if not minority_map:
        return
    split_point_event_id = min(minority_map.keys())
    minority_key = _continuation_key(root_key, split_point_event_id)

    session.upsert(minority_key).bin(BIN_NAME).set_to(minority_map).execute()
    (
        session.upsert(root_key)
        .bin(CONTINUATION_BIN).list_create(order=ListOrderType.ORDERED)
        .bin(CONTINUATION_BIN).list_append(split_point_event_id, unique=True, no_fail=True)
        .execute()
    )


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
            root_key = EVENTS.id(f"{account_id}:{record_key}")
            _process_root_and_continuations(
                session, root_key, earliest_event_id, latest_event_id,
                count, results, direction, device_filter,
            )
            record_key += 1
    else:
        record_key = end_record
        while len(results) < count and record_key >= start_record:
            root_key = EVENTS.id(f"{account_id}:{record_key}")
            _process_root_and_continuations(
                session, root_key, earliest_event_id, latest_event_id,
                count, results, direction, device_filter,
            )
            record_key -= 1

    return results


def _process_root_and_continuations(
    session: SyncSession, root_key, earliest_event_id: str, latest_event_id: str,
    count: int, results: list[Event], direction: str, device_filter: set[str] | None,
) -> None:
    row = session.query(root_key).execute().first()
    if row is None or row.record is None:
        return
    continuation = list(row.record.bins.get(CONTINUATION_BIN) or [])

    entries = _read_filtered_bucket(session, root_key, earliest_event_id, latest_event_id, device_filter)
    _add_events_to_results(count, entries, results, direction)

    if not continuation:
        return

    if direction == ASCENDING:
        for sub_key in continuation:
            if sub_key > latest_event_id or len(results) >= count:
                break
            sub_record_key = _continuation_key(root_key, sub_key)
            entries = _read_filtered_bucket(session, sub_record_key, earliest_event_id, latest_event_id, device_filter)
            _add_events_to_results(count, entries, results, direction)
    else:
        for sub_key in reversed(continuation):
            sub_record_key = _continuation_key(root_key, sub_key)
            entries = _read_filtered_bucket(session, sub_record_key, earliest_event_id, latest_event_id, device_filter)
            _add_events_to_results(count, entries, results, direction)
            if sub_key < earliest_event_id or len(results) >= count:
                break


def get_events_before(
    session: SyncSession, account_id: str, event_id: str | None = None,
    count: int = 50, device_ids: Sequence[str] = (),
) -> list[Event]:
    return get_events_between(session, account_id, None, None, event_id, count, DESCENDING, device_ids)


def get_events_after(
    session: SyncSession, account_id: str, event_id: str,
    count: int = 50, device_ids: Sequence[str] = (),
) -> list[Event]:
    return get_events_between(session, account_id, None, None, event_id, count, ASCENDING, device_ids)


def _read_filtered_bucket(
    session: SyncSession, key, earliest_event_id: str, latest_event_id: str,
    device_filter: set[str] | None,
) -> list:
    """See time_series_demo.py's ``_read_filtered_bucket`` for the native-CDT-range-plus-
    client-side-device-filter rationale (this module's ``_split_bucket`` docstring covers the
    separate AEL limitation hit while porting the write side)."""
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
    return _build_event(account_id, device_id, timestamp_ms)


def _build_event(account_id: str, device_id: str, timestamp_ms: int) -> Event:
    random_value = random.randrange(999999999999)
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
            timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - _MAX_DAYS_TO_STORE * _MILLIS_PER_DAY
            for _ in range(_NUM_EVENTS_ACCT_1):
                timestamp_ms += random.randrange(100)
                device_id = random.randrange(account.num_devices)
                event = _build_event(account.id, f"device-{account.id}-{device_id}", timestamp_ms)
                upsert_event(session, event, True)
                events_created += 1
        else:
            for device_num in range(account.num_devices):
                events_this_device = random.randrange(_MAX_EVENTS_PER_DEVICE)
                for _ in range(events_this_device):
                    event = generate_sample_event(account.id, f"device-{account.id}-{device_num}")
                    upsert_event(session, event, True)
                    events_created += 1
                devices_created += 1

        if account_num % 10 == 0 or account_num == _NUM_ACCOUNTS:
            print(f"{account_num:,} accounts, {devices_created:,} devices, {events_created:,} events")


def _demonstrate_queries(session: SyncSession) -> None:
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
    while len(events) == page_size and page_counter < 5:
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

    print("Showing events for acct-2, all devices")
    events = get_events_before(session, "acct-2", None, 20_000)
    display_events(events)


class TimeSeriesLargeVarianceDemo(UseCase):
    def get_name(self) -> str:
        return "Time-series data with large variation"

    def get_description(self) -> str:
        return (
            "Demonstrates how to store, update and query time-series data when there can be a large disparity "
            "in the events for devices. This is applicable to many ad-hoc time series events like identifying fraud in "
            "credit card swipes. (Consumers might do 20 a day, businesses could do 100,000). In this case the data is "
            "devices which generate events. These devices could be motion sensors, cameras, etc. "
            "The data model has many accounts, each account has a handful of devices, and the devices "
            "generate events when triggered. The events are stored for 14 days, and queries can be "
            "performed on the events for an account, filtering by time range and / or a list of device ids. "
            "This shows a way to store time series data with events occurring on a sporadic (random) basis with "
            "high variability in cardinality."
        )

    def get_reference(self) -> str:
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/timeseries-large-variance.md"

    def setup(self, session: SyncSession) -> None:
        session.truncate(EVENTS)
        _generate_sample_data(session)

    def run(self, session: SyncSession) -> None:
        _demonstrate_queries(session)
