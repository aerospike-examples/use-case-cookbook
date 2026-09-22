"""Port of ../../java-sdk's Async.java (itself carried over from ../../java) - a small
helper for running concurrent simulations (hot-key tests, inventory sims, etc.) for a
fixed wall-clock duration, with an optional virtual/compressed time axis for generating
realistic-looking historical data quickly. Threads (not asyncio) are used throughout since
this SDK's ``sync`` façade has no per-call event loop to share - see ../README.md.
"""

import os
import random
import threading
import time
import traceback
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone


class Async:
    @staticmethod
    def run_for(duration_seconds: float, main_thread: Callable[["Async"], None]) -> None:
        """Set up an async execution environment which persists for ``duration_seconds``
        and then automatically terminates."""
        runner = Async(duration_seconds)
        try:
            main_thread(runner)
        finally:
            runner._wait_until_done()

    def __init__(self, duration_seconds: float):
        self._start = time.monotonic()
        self._end = self._start + duration_seconds
        self._should_terminate = threading.Event()
        self._executor = ThreadPoolExecutor(max_workers=256)
        self._futures: list = []
        self._time_divisor: float | None = None
        self._start_virtual_time_monotonic: float | None = None
        self._start_physical_time: float | None = None

    def time_remaining_seconds(self) -> float:
        return max(0.0, self._end - time.monotonic())

    def use_virtual_time(
        self, logical_time: timedelta, physical_time: timedelta,
        start_offset: timedelta | None = None,
    ) -> "Async":
        self._time_divisor = max(
            1.0, logical_time.total_seconds() / physical_time.total_seconds()
        )
        self._start_physical_time = time.time() - (start_offset.total_seconds() if start_offset else 0)
        self._start_virtual_time_monotonic = time.monotonic()
        return self

    def virtual_time(self) -> float:
        """Current virtual time, as Unix-epoch seconds."""
        now = time.monotonic()
        return (now - self._start_virtual_time_monotonic) * self._time_divisor + self._start_physical_time

    def virtual_time_with_variance(self, min_variance_seconds: float, max_variance_seconds: float) -> float:
        return self.virtual_time() + random.uniform(min_variance_seconds, max_variance_seconds)

    def virtual_date(self) -> datetime:
        return datetime.fromtimestamp(self.virtual_time(), tz=timezone.utc)

    def virtual_date_with_variance(self, min_variance_seconds: float, max_variance_seconds: float) -> datetime:
        return datetime.fromtimestamp(
            self.virtual_time_with_variance(min_variance_seconds, max_variance_seconds), tz=timezone.utc,
        )

    def terminate(self) -> "Async":
        self._should_terminate.set()
        return self

    def done(self) -> bool:
        return self._should_terminate.is_set()

    def _num_copies_to_use(self, requested_copies: int) -> int:
        if requested_copies <= 0:
            return max(1, (os.cpu_count() or 1) + requested_copies)
        return requested_copies

    def continuous(self, runner: Callable[[], None], number_of_copies: int = 1) -> "Async":
        """Runs ``runner`` in a tight loop on ``number_of_copies`` threads until the
        duration elapses. ``runner`` should not loop indefinitely itself - it's called
        over and over. Specify 0 for the number of available processors, -1 for one less
        than that, etc. ``runner`` must be thread-safe.
        """
        n = self._num_copies_to_use(number_of_copies)

        def _loop() -> None:
            try:
                while not self._should_terminate.is_set():
                    runner()
            except Exception:  # noqa: BLE001 - thread-boundary catch, one bad iteration shouldn't kill the loop
                print("Continuously executing thread threw unhandled exception:")
                traceback.print_exc()

        for _ in range(n):
            self._futures.append(self._executor.submit(_loop))
        return self

    def periodic(self, period_seconds: float, runner: Callable[[], None], number_of_copies: int = 1) -> "Async":
        """Runs ``runner`` every ``period_seconds`` on ``number_of_copies`` threads until
        the duration elapses, each thread jittered with a random initial delay.
        """
        n = self._num_copies_to_use(number_of_copies)

        def _loop() -> None:
            try:
                time.sleep(random.uniform(0, period_seconds))
                while not self._should_terminate.is_set():
                    runner()
                    time.sleep(period_seconds)
            except Exception:  # noqa: BLE001 - thread-boundary catch, one bad iteration shouldn't kill the loop
                print("Periodic thread threw unhandled exception:")
                traceback.print_exc()

        for _ in range(n):
            self._futures.append(self._executor.submit(_loop))
        return self

    def _wait_until_done(self) -> None:
        remaining = self.time_remaining_seconds()
        if remaining > 0:
            time.sleep(remaining)
        self._should_terminate.set()
        self._executor.shutdown(wait=True)
