"""Port of ../../../java-sdk's hotkeys/WriteHotKeyUseCase.java (see ../../java). Demonstrates a
write hot key: many threads increment ``unitsSold`` on the same product.

Baseline: all writes target the primary key ``product_id``.
Mitigation: writes are directed to a random shard ``product_id:index``; a batch read merges
``unitsSold`` across shards when the logical total is needed.

Both phases also read the logical total about once every 5ms so occasional reads occur
alongside the write-heavy load.
"""

from aerospike_sdk.sync import Session

from usecasecookbook import config
from usecasecookbook.hotkeys import keys as hot_key_keys
from usecasecookbook.hotkeys import pending_limit_scope, product_setup, simulation
from usecasecookbook.use_case import UseCase


class WriteHotKeyUseCase(UseCase):
    def get_name(self) -> str:
        return "Hot Key - Write (Shard + Merge)"

    def get_description(self) -> str:
        return (
            "Simulate a write hot key by incrementing unitsSold on one record, then repeat with "
            "sharded writes and batch-read merge to reconstruct the logical total."
        )

    def get_reference(self) -> str:
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/hot-key-write-shard-merge.md"

    def setup(self, session: Session) -> None:
        product_setup.truncate_and_seed(session, simulation.REPLICA_COUNT)

    def run(self, session: Session) -> None:
        product_id = simulation.HOT_PRODUCT_ID
        num_threads = simulation.NUM_THREADS
        duration_secs = simulation.DURATION_SECS
        replica_count = simulation.REPLICA_COUNT
        side_op_interval_secs = simulation.PERIODIC_SIDE_OP_INTERVAL_SECS

        primary_key = hot_key_keys.primary(product_id)

        def write_primary() -> None:
            stream = session.upsert(primary_key).bin("unitsSold").add(1).execute()
            stream.close()

        def read_primary_total() -> None:
            stream = session.query(primary_key).bins(["unitsSold"]).execute()
            stream.close()

        def write_random_shard() -> None:
            shard_key = hot_key_keys.random_replica(product_id, replica_count)
            stream = session.upsert(shard_key).bin("unitsSold").add(1).execute()
            stream.close()

        def read_merged_total() -> None:
            product_setup.read_merged_units_sold(session, replica_count)

        with pending_limit_scope.apply(session, config.NAMESPACE, simulation.TRANSACTION_PENDING_LIMIT):
            baseline = simulation.run_simulation(
                "Baseline - single write hot key", num_threads, duration_secs, write_primary,
                read_primary_total, side_op_interval_secs,
            )

            print(f"Primary record unitsSold after baseline: {product_setup.read_units_sold(session, primary_key)}")

            mitigated = simulation.run_simulation(
                "Mitigation - random write shard", num_threads, duration_secs, write_random_shard,
                read_merged_total, side_op_interval_secs,
            )

            merged_total = product_setup.read_merged_units_sold(session, replica_count)
            print(f"Merged unitsSold across {replica_count} shards after mitigation: {merged_total}")
            print(f"Mitigation successes: {mitigated.successes:,} (baseline successes: {baseline.successes:,})")
