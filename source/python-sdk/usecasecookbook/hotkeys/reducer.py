"""Port of ../../../java-sdk's hotkeys/helper/HotKeyReducer.java (itself vendored from the
standalone hot-key-reducer project, see ../../java's hotkeys.helper package). Batches
concurrent operate calls against the same hot key within the process into a single Aerospike
operate call, then unpacks the combined result back to each caller.

The Java SDK port replaced the legacy client's per-bin-name counting/index bookkeeping with a
simple index-range slice of ``Record.results[]`` (per-operation results in submission order,
regardless of bin name). This SDK has no equivalent: ``aerospike_async.Record`` only exposes
``bins`` (a dict keyed by bin name) - there is no positional per-operation results array
(confirmed both by reading ``aerospike_sdk.operation_result``'s module docstring - "A positional
Record.results array indexed in operation order is not yet exposed by the underlying async
client" - and empirically: a merged operate call touching the same bin name N times comes back
as ``bins[name] == [v0, v1, ..., vN-1]`` in submission order, exactly like the legacy client's
behavior). So this port keeps the legacy client's per-bin-name counting approach: for each bin
name, track how many total operations reference it across the whole batch and how many of those
belong to this caller, then slice the (possibly list-valued) aggregated result accordingly.

A further, real API difference from both prior ports: ``aerospike_async.Operation`` objects are
opaque (no ``bin_name`` accessor, unlike the legacy client's public ``Operation.binName`` field),
so the bin-name bookkeeping can't be recovered by inspecting an ``Operation`` after the fact.
``submit()`` therefore takes ``(bin_name, Operation)`` pairs instead of raw ``Operation`` objects
- the caller already knows the bin name (it built the ``Operation`` from it), so this costs
nothing at the call site while letting the reducer do the same counting the legacy client did.

Raw ``Operation`` objects are merged into one write via ``WriteSegmentBuilder._add_op`` - there is
no public "append pre-built Operation" method (unlike the legacy/Java-SDK clients' public
``operate(...)``/``appendOperations(...)``), so this uses that one leading-underscore method,
which mirrors exactly what the public bin-builder chain (``.bin(x).add(1)``) does internally.
"""

import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from aerospike_async import Key, Operation
from aerospike_sdk import SyncSession

BinOp = Tuple[str, Operation]


class IllegalStateError(RuntimeError):
    """Raised for HotKeyReducer internal invariant violations."""


class _PendingResult:
    """A simple thread-safe single-value future - stands in for Java's CompletableFuture."""

    __slots__ = ("_event", "_value", "_error")

    def __init__(self) -> None:
        self._event = threading.Event()
        self._value: Optional[Dict[str, Any]] = None
        self._error: Optional[Exception] = None

    def finish(self, value: Optional[Dict[str, Any]]) -> None:
        self._value = value
        self._event.set()

    def fail(self, error: Exception) -> None:
        self._error = error
        self._event.set()

    def wait(self) -> Optional[Dict[str, Any]]:
        self._event.wait()
        if self._error is not None:
            raise self._error
        return self._value


class _SubmittedOp:
    __slots__ = ("bin_name", "operation", "bin_index")

    def __init__(self, bin_name: str, operation: Operation, bin_index: int) -> None:
        self.bin_name = bin_name
        self.operation = operation
        # Index of this op among ALL ops (from every caller in the batch) touching bin_name,
        # in submission order - matches the position this op's value will occupy in the
        # aggregated record's per-bin list once the batch executes.
        self.bin_index = bin_index


class _SubmittedOps:
    """One caller's submission within a batch - tracks each of its ops' position among all
    ops sharing the same bin name across the whole batch.
    """

    __slots__ = ("pending", "ops", "this_bin_counts")

    def __init__(self, pending: _PendingResult, all_bin_counts: Dict[str, int], bin_ops: Sequence[BinOp]) -> None:
        self.pending = pending
        self.ops: List[_SubmittedOp] = []
        self.this_bin_counts: Dict[str, int] = {}
        for bin_name, operation in bin_ops:
            bin_index = all_bin_counts.get(bin_name, 0)
            all_bin_counts[bin_name] = bin_index + 1
            self.this_bin_counts[bin_name] = self.this_bin_counts.get(bin_name, 0) + 1
            self.ops.append(_SubmittedOp(bin_name, operation, bin_index))

    def finish(self, aggregated_bins: Dict[str, Any], all_bin_counts: Dict[str, int]) -> None:
        """Extract this caller's slice of the combined result.

        Mirrors the legacy client's ``SubmittedOps.formRecordForTheseOps``: a bin touched once
        across the whole batch comes back as a scalar; touched more than once, it comes back as
        a list (in submission order) and each caller picks out its own index (or indices, if the
        same caller touched the same bin more than once in its own submission).
        """
        result: Dict[str, Any] = {}
        for op in self.ops:
            total_count = all_bin_counts.get(op.bin_name, 0)
            this_count = self.this_bin_counts.get(op.bin_name, 0)
            value = aggregated_bins.get(op.bin_name)
            if total_count <= 1:
                result[op.bin_name] = value
                continue
            value_list = value if isinstance(value, list) else [value]
            if op.bin_index >= len(value_list):
                continue
            element = value_list[op.bin_index]
            if element is None:
                continue
            if this_count == 1:
                result[op.bin_name] = element
            else:
                result.setdefault(op.bin_name, []).append(element)
        self.pending.finish(result if result else None)

    def fail(self, error: Exception) -> None:
        self.pending.fail(error)


class _OperationBatch:
    """All operations for a single key submitted within a batching time window."""

    __slots__ = ("all_bin_counts", "calls")

    def __init__(self) -> None:
        self.all_bin_counts: Dict[str, int] = {}
        self.calls: List[_SubmittedOps] = []

    def submit_call(self, pending: _PendingResult, bin_ops: Sequence[BinOp]) -> None:
        self.calls.append(_SubmittedOps(pending, self.all_bin_counts, bin_ops))

    def all_operations(self) -> List[Operation]:
        ops: List[Operation] = []
        for call in self.calls:
            ops.extend(sop.operation for sop in call.ops)
        return ops

    def is_single_call(self) -> bool:
        return len(self.calls) == 1

    def finish(self, aggregated_bins: Dict[str, Any]) -> None:
        for call in self.calls:
            call.finish(aggregated_bins, self.all_bin_counts)

    def fail(self, error: Exception) -> None:
        for call in self.calls:
            call.fail(error)


class _KeyStats:
    __slots__ = ("expiry_ms", "counter")

    def __init__(self) -> None:
        self.expiry_ms = 0
        self.counter = 0


class _ReducerMonitor:
    """Monitors key access patterns to determine when the reducer should batch a key's
    operations. Keyed by ``key.digest`` (a hashable str) rather than ``Key`` itself, since
    ``Key`` is not hashable in this SDK (confirmed empirically: ``hash(key)`` raises TypeError).
    """

    def __init__(self, accesses_per_ms_for_hot: int, ms_to_keep_hot: int) -> None:
        self._accesses_per_ms_for_hot = accesses_per_ms_for_hot
        self._ms_to_keep_hot = ms_to_keep_hot
        self._current: Dict[str, _KeyStats] = {}
        self._previous: Dict[str, _KeyStats] = {}
        self._lock = threading.Lock()
        self.hot_key_accesses = 0
        self.non_hot_key_accesses = 0

        self._stop = threading.Event()
        self._time_tracker_thread: Optional[threading.Thread] = None
        if accesses_per_ms_for_hot > 1:
            self._time_tracker_thread = threading.Thread(target=self._time_tracker, daemon=True)
            self._time_tracker_thread.start()
        # Else: each access would tag a key as hot, so this functionality is disabled.

    def _time_tracker(self) -> None:
        while not self._stop.wait(1.0):
            with self._lock:
                self._previous = self._current
                self._current = {}

    def _stats_for_key(self, key_digest: str) -> _KeyStats:
        # Caller must hold self._lock.
        stats = self._current.get(key_digest)
        if stats is not None:
            return stats
        stats = self._previous.pop(key_digest, None)
        if stats is not None:
            self._current[key_digest] = stats
            return stats
        stats = _KeyStats()
        self._current[key_digest] = stats
        return stats

    def use_reducer(self, key: Key) -> bool:
        if self._time_tracker_thread is None:
            result = True
        else:
            now = int(time.monotonic() * 1000)
            with self._lock:
                stats = self._stats_for_key(key.digest)
                if stats.expiry_ms < now:
                    stats.expiry_ms = now
                    stats.counter = 1
                    result = False
                elif stats.expiry_ms == now or (stats.expiry_ms + 1) == now:
                    stats.counter += 1
                    if stats.counter >= self._accesses_per_ms_for_hot:
                        stats.expiry_ms = now + self._ms_to_keep_hot
                        result = True
                    else:
                        stats.expiry_ms = now
                        result = False
                else:
                    stats.expiry_ms = now + self._ms_to_keep_hot
                    result = True

        with self._lock:
            if result:
                self.hot_key_accesses += 1
            else:
                self.non_hot_key_accesses += 1
        return result


class HotKeyReducerStatistics:
    """Statistics about the reducer's hot-key detection effectiveness and delay timing."""

    def __init__(
        self, hot_key_accesses: int, non_hot_key_accesses: int, desired_delay_secs: float, actual_delay_secs: float,
    ) -> None:
        self.hot_key_accesses = hot_key_accesses
        self.non_hot_key_accesses = non_hot_key_accesses
        self.desired_delay_secs = desired_delay_secs
        self.actual_delay_secs = actual_delay_secs

    def __str__(self) -> str:
        return (
            f"Statistics [hotKeyAccesses={self.hot_key_accesses}, "
            f"nonHotKeyAccesses={self.non_hot_key_accesses}, "
            f"desiredDelayTime={self.desired_delay_secs * 1000:.3f}ms, "
            f"actualDelayTime={self.actual_delay_secs * 1_000_000:.1f}us]"
        )


class HotKeyReducer:
    """Creates a HotKeyReducer with full configuration options.

    Args:
        session: the SyncSession to use for the batched operate calls.
        delay_secs: time to wait before executing batched operations (minimum 1 microsecond).
        accesses_per_ms_for_hot: minimum accesses per millisecond for a key to be considered hot.
        ms_to_keep_hot: duration in milliseconds to keep a key hot once detected.
    """

    def __init__(self, session: SyncSession, delay_secs: float, accesses_per_ms_for_hot: int, ms_to_keep_hot: int) -> None:
        if delay_secs < 1e-6:
            raise ValueError(f"Delay time must be at least 1us, not {delay_secs}s")

        self._session = session
        self._delay_secs = delay_secs
        self._monitor = _ReducerMonitor(accesses_per_ms_for_hot, ms_to_keep_hot)
        # Keyed by key.digest (a hashable str), not Key itself - see _ReducerMonitor.
        self._batches: Dict[str, _OperationBatch] = {}
        self._batches_lock = threading.Lock()

        start = time.monotonic()
        time.sleep(delay_secs)
        self._actual_delay_secs = time.monotonic() - start

    def _add_ops_for_key(self, key: Key, pending: _PendingResult, bin_ops: Sequence[BinOp]) -> bool:
        with self._batches_lock:
            batch = self._batches.get(key.digest)
            should_delay = False
            if batch is None:
                batch = _OperationBatch()
                self._batches[key.digest] = batch
                should_delay = True
            batch.submit_call(pending, bin_ops)
            return should_delay

    def _take_batch(self, key: Key) -> _OperationBatch:
        with self._batches_lock:
            batch = self._batches.pop(key.digest, None)
        if batch is None:
            raise IllegalStateError(f"No batch found for key {key} - this is a HotKeyReducer bug.")
        return batch

    def submit(self, key: Key, *bin_ops: BinOp) -> Optional[Dict[str, Any]]:
        """Submits operations for execution, batching them with other operations for the same
        key if it's determined to be hot. Blocks until the operation (or its batch) executes.

        Args:
            key: the Aerospike key to operate on.
            bin_ops: ``(bin_name, Operation)`` pairs to perform.

        Returns:
            This caller's slice of the (possibly merged) result, or ``None`` if no bin came back.
        """
        pending = _PendingResult()
        batch_owned: Optional[_OperationBatch] = None
        try:
            if not self._monitor.use_reducer(key):
                builder = self._session.upsert(key)
                for _, op in bin_ops:
                    builder = builder._add_op(op)
                record_result = builder.execute().first()
                bins = record_result.record.bins if record_result is not None and record_result.record is not None else None
                pending.finish(bins if bins else None)
            elif self._add_ops_for_key(key, pending, bin_ops):
                time.sleep(self._delay_secs)
                batch_owned = self._take_batch(key)
                builder = self._session.upsert(key)
                for op in batch_owned.all_operations():
                    builder = builder._add_op(op)
                record_result = builder.execute().first()
                bins = record_result.record.bins if record_result is not None and record_result.record is not None else {}
                if batch_owned.is_single_call():
                    pending.finish(bins if bins else None)
                else:
                    batch_owned.finish(bins)
        except Exception as e:
            if batch_owned is not None:
                batch_owned.fail(e)
            else:
                pending.fail(e)
        return pending.wait()

    def get_statistics(self) -> HotKeyReducerStatistics:
        return HotKeyReducerStatistics(
            self._monitor.hot_key_accesses, self._monitor.non_hot_key_accesses,
            self._delay_secs, self._actual_delay_secs,
        )
