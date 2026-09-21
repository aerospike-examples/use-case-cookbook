"""Cluster capability shim for multi-record transactions.

Mirrors ../../java-sdk's NonTransactionalCapableSession/NonTransactionalSession, but far
more simply: this SDK's ``Session.is_namespace_sc(namespace)`` tells us directly
whether the connected namespace supports strong consistency (required for real
multi-record transactions on Aerospike 8+), so no throwaway-write probe is needed. And
because ``do_in_transaction``'s callback only needs the ``upsert``/``query``/``update``/
``delete``/etc. methods that ``Session`` and ``SyncTransactionalSession`` share, running
the callback directly against the plain session (skipping ``do_in_transaction`` entirely)
reproduces the same "no atomicity guarantee, but it runs" behavior without needing a
subclass or proxy at all.

Every transactional use case should call :func:`run_in_transaction` instead of
``session.do_in_transaction`` directly.
"""

from collections.abc import Callable
from typing import TypeVar

from aerospike_sdk.sync import Session

from usecasecookbook import config
from usecasecookbook.ansi_colors import RESET, YELLOW

T = TypeVar("T")

_warned = False


def transactions_supported(session: Session) -> bool:
    return session.is_namespace_sc(config.NAMESPACE)


def run_in_transaction(session: Session, operation: Callable[[Session], T]) -> T:
    """Run ``operation(tx_session)`` in a real transaction if the namespace supports
    strong consistency; otherwise run it directly against ``session`` with a one-time
    warning, and no atomicity guarantee.
    """
    if transactions_supported(session):
        return session.do_in_transaction(operation)

    global _warned
    if not _warned:
        print(
            f"{YELLOW}Note: this cluster/namespace does not support multi-record "
            "transactions (requires Aerospike 8+ with a strong-consistency-enabled "
            "namespace). Transaction-based use cases will still run, but without the "
            f"atomicity guarantee a real transaction would provide.{RESET}"
        )
        _warned = True
    return operation(session)
