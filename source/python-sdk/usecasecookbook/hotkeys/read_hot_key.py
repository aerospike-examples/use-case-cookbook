"""Port of ../../../java-sdk's hotkeys/ReadHotKeyUseCase.java (see ../../java). Demonstrates a
read hot key: many threads repeatedly read the same product record.

Baseline: all reads target the primary key ``product_id``.
Mitigation: identical copies are stored at ``product_id:0..N-1``; each read picks a random
replica so load is spread across multiple records.

Both phases also increment ``unitsSold`` on the primary key and every replica (~every 5ms) via
a batch write so all copies stay in sync alongside the read-heavy load.
"""

from aerospike_sdk.sync import Session

from usecasecookbook import config
from usecasecookbook.hotkeys import keys as hot_key_keys
from usecasecookbook.hotkeys import pending_limit_scope, product_setup, simulation
from usecasecookbook.use_case import UseCase


class ReadHotKeyUseCase(UseCase):
    def get_name(self) -> str:
        return "Hot Key - Read (Replica Spread)"

    def get_description(self) -> str:
        return (
            "Simulate a read hot key against a single product record, then repeat with identical "
            "replica records and random replica selection to spread read load."
        )

    def get_reference(self) -> str:
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/hot-key-read-replica-spread.md"

    def setup(self, session: Session) -> None:
        product_setup.truncate_and_seed(session, simulation.REPLICA_COUNT)

    def run(self, session: Session) -> None:
        product_id = simulation.HOT_PRODUCT_ID
        num_threads = simulation.NUM_THREADS
        duration_secs = simulation.DURATION_SECS
        replica_count = simulation.REPLICA_COUNT
        side_op_interval_secs = simulation.PERIODIC_SIDE_OP_INTERVAL_SECS

        primary_key = hot_key_keys.primary(product_id)

        def refresh_all_copies() -> None:
            product_setup.increment_units_sold_on_all_copies(session, replica_count)

        def read_primary() -> None:
            stream = session.query(primary_key).bins(["sku", "description", "unitsSold"]).execute()
            stream.close()

        def read_random_replica() -> None:
            replica_key = hot_key_keys.random_replica(product_id, replica_count)
            stream = session.query(replica_key).bins(["sku", "description", "unitsSold"]).execute()
            stream.close()

        with pending_limit_scope.apply(session, config.NAMESPACE, simulation.TRANSACTION_PENDING_LIMIT):
            simulation.run_simulation(
                "Baseline - single read hot key", num_threads, duration_secs, read_primary,
                refresh_all_copies, side_op_interval_secs,
            )
            simulation.run_simulation(
                "Mitigation - random read replica", num_threads, duration_secs, read_random_replica,
                refresh_all_copies, side_op_interval_secs,
            )
