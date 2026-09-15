"""Port of ../../../java-sdk's hotkeys/ReducerHotKeyUseCase.java (see ../../java).
Demonstrates programmatic hot-key reduction: concurrent operate calls against one key are
batched inside the process by HotKeyReducer before being sent to Aerospike.

Baseline: each thread calls ``session.upsert(key)...execute()`` directly on the hot key.
Mitigation: the same operations are submitted through HotKeyReducer, which coalesces
operations for hot keys within a short delay window.

REVIEW: Tuning REDUCER_DELAY_SECS, HOT_THRESHOLD, and HOT_DURATION_MS strongly affects batching
behaviour and latency. Adjust these for your workload and cluster settings.
"""

from aerospike_async import Operation
from aerospike_sdk import SyncSession

from usecasecookbook import config
from usecasecookbook.hotkeys import keys as hot_key_keys
from usecasecookbook.hotkeys import pending_limit_scope, product_setup, simulation
from usecasecookbook.hotkeys.reducer import HotKeyReducer
from usecasecookbook.use_case import UseCase

# REVIEW: delay before flushing a batch - lower = less latency, less batching opportunity
REDUCER_DELAY_SECS = 0.001
# REVIEW: accesses in the same millisecond before a key is treated as hot
HOT_THRESHOLD = 3
# REVIEW: how long a key stays hot once detected
HOT_DURATION_MS = 500


class ReducerHotKeyUseCase(UseCase):
    def get_name(self) -> str:
        return "Hot Key - Write (HotKeyReducer)"

    def get_description(self) -> str:
        return (
            "Simulate a write hot key with direct operate calls, then repeat using HotKeyReducer "
            "to batch concurrent operations on the same key within the process."
        )

    def get_reference(self) -> str:
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/hot-key-write-reducer.md"

    def setup(self, session: SyncSession) -> None:
        # Reducer operates on a single key; replicas are not used but primary is seeded cleanly.
        product_setup.truncate_and_seed(session, 1)

    def run(self, session: SyncSession) -> None:
        product_id = simulation.HOT_PRODUCT_ID
        num_threads = simulation.NUM_THREADS
        duration_secs = simulation.DURATION_SECS

        primary_key = hot_key_keys.primary(product_id)

        def write_direct() -> None:
            stream = session.upsert(primary_key).bin("unitsSold").add(1).execute()
            stream.close()

        with pending_limit_scope.apply(session, config.NAMESPACE, simulation.TRANSACTION_PENDING_LIMIT):
            baseline = simulation.run_simulation(
                "Baseline - direct operate on hot key", num_threads, duration_secs, write_direct,
            )

            # REVIEW: HotKeyReducer is vendored from hot-key-reducer; verify delay/threshold for your cluster.
            reducer = HotKeyReducer(session, REDUCER_DELAY_SECS, HOT_THRESHOLD, HOT_DURATION_MS)

            def write_via_reducer() -> None:
                reducer.submit(primary_key, ("unitsSold", Operation.add("unitsSold", 1)))

            mitigated = simulation.run_simulation(
                "Mitigation - HotKeyReducer batching", num_threads, duration_secs, write_via_reducer,
            )

            print(f"HotKeyReducer statistics: {reducer.get_statistics()}")
            print(f"Primary record unitsSold after mitigation: {product_setup.read_units_sold(session, primary_key)}")
            print(f"Mitigation successes: {mitigated.successes:,} (baseline successes: {baseline.successes:,})")
