"""Mirrors ../../../java-sdk's hotkeys/HotKeyPendingLimitScope.java. Temporarily sets
``transaction-pending-limit`` on the hot-key namespace via info ``set-config``, restoring the
previous value on exit.
"""

from contextlib import contextmanager

from aerospike_sdk.sync import Session


def _parse_info_value(raw: str, key: str) -> str:
    for kv in raw.split(";"):
        if "=" in kv:
            k, v = kv.split("=", 1)
            if k == key:
                return v
    raise KeyError(f"'{key}' not found in info response: {raw!r}")


def read_transaction_pending_limit(session: Session, namespace: str) -> int:
    # session.info().namespace_details(namespace) returns a structured NamespaceDetail with a
    # fixed set of fields (keys/exists/strong_consistency/nsup_period) that doesn't include
    # transaction-pending-limit, so this reads the raw get-config response directly instead.
    command = f"get-config:context=namespace;id={namespace}"
    response = session.info().info(command)
    return int(_parse_info_value(response[command], "transaction-pending-limit"))


def _set_transaction_pending_limit(session: Session, namespace: str, pending_limit: int) -> None:
    command = f"set-config:context=namespace;id={namespace};transaction-pending-limit={pending_limit}"
    responses = session.info().info_on_all_nodes(command)
    for node_name, node_response in responses.items():
        result = node_response.get(command, "").strip().lower()
        if result != "ok":
            raise RuntimeError(f"Failed to set transaction-pending-limit on node {node_name}: {result}")


@contextmanager
def apply(session: Session, namespace: str, pending_limit: int):
    """Reads the current namespace limit, applies ``pending_limit`` on every cluster node, and
    restores the original value when the context manager exits.
    """
    if pending_limit < 0:
        raise ValueError("transaction_pending_limit must be >= 0 (0 disables the queue check)")

    previous_limit = read_transaction_pending_limit(session, namespace)
    _set_transaction_pending_limit(session, namespace, pending_limit)
    print(
        f"Namespace '{namespace}' transaction-pending-limit: {previous_limit} -> {pending_limit} "
        f"(restored to {previous_limit} when the use case finishes)"
    )
    try:
        yield
    finally:
        _set_transaction_pending_limit(session, namespace, previous_limit)
        print(f"Restored namespace '{namespace}' transaction-pending-limit to {previous_limit}")
