"""Mirrors ../../../java-sdk's hotkeys/HotKeySimulationParams.java,
HotKeySimulationStats.java, and HotKeySimulation.java.

Pure Python - no Aerospike-specific or ``usecasecookbook.async_util.Async`` dependency, since
these run a fixed-thread-count load phase for a fixed duration rather than the
continuous/periodic-until-terminated shape ``Async`` provides.
"""

import threading
import time
import traceback
from collections import Counter
from dataclasses import dataclass, field
from typing import Callable, Optional

from aerospike_sdk.exceptions import AerospikeError
from aerospike_sdk.exceptions import ResultCode

# Shared simulation parameters for the hot-key use cases.
NUM_THREADS = 25
DURATION_SECS = 10
REPLICA_COUNT = 4

# Namespace transaction-pending-limit applied via info set-config for the simulation (lower
# values make KEY_BUSY easier to observe; 0 disables the check).
TRANSACTION_PENDING_LIMIT = 20

# Fixed logical product id for the single hot-key record used by all demonstrations.
HOT_PRODUCT_ID = 1

# Interval for occasional cross-traffic (writes during read load, reads during write load).
PERIODIC_SIDE_OP_INTERVAL_SECS = 0.005

OperationAttempt = Callable[[], None]


@dataclass
class HotKeySimulationStats:
    """Thread-safe-enough counters for a hot-key simulation phase (protected by a lock since
    Python has no built-in atomic counter type). Latency is tracked only for successful
    operations.
    """

    attempts: int = 0
    successes: int = 0
    key_busy_errors: int = 0
    other_errors: int = 0
    success_latency_secs: float = 0.0
    other_error_counts: Counter = field(default_factory=Counter)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    def record_attempt(self) -> None:
        with self._lock:
            self.attempts += 1

    def record_success(self, latency_secs: float) -> None:
        with self._lock:
            self.successes += 1
            self.success_latency_secs += latency_secs

    def record_key_busy(self) -> None:
        with self._lock:
            self.key_busy_errors += 1

    def record_other_error(self, error_type: str = "Unknown") -> None:
        with self._lock:
            self.other_errors += 1
            self.other_error_counts[error_type] += 1

    def average_success_latency_ms(self) -> float:
        with self._lock:
            if self.successes == 0:
                return 0.0
            return self.success_latency_secs * 1000.0 / self.successes

    def snapshot_delta(self, previous: "HotKeySimulationStats") -> "HotKeySimulationStats":
        with self._lock, previous._lock:
            delta = HotKeySimulationStats()
            delta.attempts = self.attempts - previous.attempts
            delta.successes = self.successes - previous.successes
            delta.key_busy_errors = self.key_busy_errors - previous.key_busy_errors
            delta.other_errors = self.other_errors - previous.other_errors
            delta.success_latency_secs = self.success_latency_secs - previous.success_latency_secs
            return delta

    def copy(self) -> "HotKeySimulationStats":
        with self._lock:
            copy = HotKeySimulationStats()
            copy.attempts = self.attempts
            copy.successes = self.successes
            copy.key_busy_errors = self.key_busy_errors
            copy.other_errors = self.other_errors
            copy.success_latency_secs = self.success_latency_secs
            return copy

    def __str__(self) -> str:
        return (
            f"attempts={self.attempts:,} successes={self.successes:,} "
            f"KEY_BUSY={self.key_busy_errors:,} otherErrors={self.other_errors:,} "
            f"avgSuccessLatency={self.average_success_latency_ms():.2f}ms"
        )

    def format_interval_line(self) -> str:
        return (
            f"  [interval] attempts={self.attempts:,} successes={self.successes:,} "
            f"KEY_BUSY={self.key_busy_errors:,} otherErrors={self.other_errors:,} "
            f"avgSuccessLatency={self.average_success_latency_ms():.2f}ms"
        )

    def print_other_error_breakdown(self) -> None:
        if self.other_errors == 0:
            return
        print("  [other errors by type]")
        for error_type, count in sorted(self.other_error_counts.items(), key=lambda kv: -kv[1]):
            print(f"    {count:,}  {error_type}")


def _describe_error(e: Exception) -> str:
    if isinstance(e, AerospikeError):
        return f"AerospikeError {e.result_code}"
    return type(e).__name__


def run_simulation(
    phase_label: str, num_threads: int, duration_secs: float, attempt: OperationAttempt,
    periodic_side_task: Optional[OperationAttempt] = None, periodic_interval_secs: float = 0,
) -> HotKeySimulationStats:
    """Executes ``num_threads`` worker threads for ``duration_secs``, invoking ``attempt`` as
    fast as possible on each thread. Optionally runs ``periodic_side_task`` on a background
    thread every ``periodic_interval_secs`` - side-task failures aren't counted in the main
    simulation statistics.
    """
    print()
    print(f"=== {phase_label} ===")
    print(f"Threads: {num_threads:,}  Duration: {duration_secs:,}s")
    if periodic_side_task is not None and periodic_interval_secs > 0:
        print(f"Periodic side operation every {periodic_interval_secs * 1000:,.0f}ms")

    totals = HotKeySimulationStats()
    previous_totals = HotKeySimulationStats()
    end_time = time.monotonic() + duration_secs
    running = threading.Event()
    running.set()

    side_op_thread = _start_periodic_side_task(periodic_side_task, periodic_interval_secs, running)

    def worker() -> None:
        while running.is_set() and time.monotonic() < end_time:
            totals.record_attempt()
            start = time.monotonic()
            try:
                attempt()
                totals.record_success(time.monotonic() - start)
            except AerospikeError as e:
                if e.result_code == ResultCode.KEY_BUSY:
                    totals.record_key_busy()
                else:
                    totals.record_other_error(_describe_error(e))
            except Exception as e:
                totals.record_other_error(_describe_error(e))

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(num_threads)]
    for t in threads:
        t.start()

    next_report = time.monotonic() + 1
    while time.monotonic() < end_time:
        time.sleep(max(0.001, next_report - time.monotonic()))
        interval = totals.snapshot_delta(previous_totals)
        print(interval.format_interval_line())
        previous_totals = totals.copy()
        next_report += 1

    running.clear()
    if side_op_thread is not None:
        side_op_thread.join(timeout=5)
    for t in threads:
        t.join(timeout=30)

    print(f"  [total]   {totals}")
    totals.print_other_error_breakdown()
    return totals


def _start_periodic_side_task(
    periodic_side_task: Optional[OperationAttempt], periodic_interval_secs: float, running: threading.Event,
) -> Optional[threading.Thread]:
    if periodic_side_task is None or periodic_interval_secs <= 0:
        return None

    def loop() -> None:
        while running.is_set():
            time.sleep(periodic_interval_secs)
            if not running.is_set():
                return
            try:
                periodic_side_task()
            except Exception:
                # Side operations are background traffic; main stats stay focused on hot-path load.
                pass

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    return thread
