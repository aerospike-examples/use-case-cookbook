"""Port of the legacy ManyToManyRelationships (see ../../java and ../../java-sdk).
Demonstrates a many-to-many relationship: each account holds a set-like list of owning
customer ids (``owners``), and each customer holds a set-like list of the accounts it owns
(``accounts``), so the relationship can be traversed from either side.

Uses ``usecasecookbook.txn.run_in_transaction`` instead of raw ``session.do_in_transaction``
(see that module - it degrades to a plain, non-atomic call on this dev cluster, which has
no strong-consistency namespace).
"""

import random
import uuid
from datetime import datetime, timedelta, timezone

from aerospike_async import ListReturnType
from aerospike_sdk import DataSet
from aerospike_sdk.sync import Session

from usecasecookbook import config
from usecasecookbook.manytomany.model import Account, Customer
from usecasecookbook.txn import run_in_transaction
from usecasecookbook.use_case import UseCase

NUM_ACCOUNTS = 2_000
NUM_CUSTOMERS = 1_000

FIRST_NAMES = ["Estefana", "Arnoldo", "Jacquelyne", "Harris", "Ardelia"]
LAST_NAMES = ["Ruecker", "MacGyver", "Willms", "Jones", "Renner"]

ACCOUNTS = DataSet.of(config.NAMESPACE, "uccb_account")
CUSTOMERS = DataSet.of(config.NAMESPACE, "uccb_customer")


def _random_customer(cust_id: str) -> Customer:
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    fifty_years = timedelta(days=50 * 365)
    ten_years = timedelta(days=10 * 365)
    dob = datetime.now(timezone.utc) - timedelta(
        seconds=random.uniform(fifty_years.total_seconds() / 2, fifty_years.total_seconds()),
    )
    date_joined = datetime.now(timezone.utc) - timedelta(seconds=random.uniform(0, ten_years.total_seconds()))
    return Customer(cust_id, first, last, dob, date_joined)


def _random_account(account_id: str) -> Account:
    balance_in_cents = random.randint(500, 2_000_000)
    five_years = timedelta(days=5 * 365)
    date_opened = datetime.now(timezone.utc) - timedelta(seconds=random.uniform(0, five_years.total_seconds()))
    return Account(account_id, f"Account {account_id}", balance_in_cents, date_opened)


def add_account(session: Session, account: Account, owner_ids: list[str]) -> bool:
    """Adds a new account and updates every owning customer's ``accounts`` list, all
    inside a transaction. Returns ``True`` if every operation succeeded.

    The legacy (non-SDK) version built a batch policy with a transaction id, then
    overwrote it back to ``None`` before the batch call - so the customer-side update
    never actually ran inside a transaction there. This port fixes that by running the
    whole thing (account write, owners bin, and the batch update of every owning
    customer) against the same transactional session.
    """

    def _op(tx: Session) -> bool:
        tx.upsert(ACCOUNTS.id(account.id)).put(account.to_bins()).execute()
        tx.upsert(ACCOUNTS.id(account.id)).bin("owners").set_to(owner_ids).execute()

        customer_keys = CUSTOMERS.ids(owner_ids)
        stream = (
            tx.upsert(customer_keys)
            .bin("accounts").list_append(account.id, unique=True, no_fail=True)
            .execute()
        )
        try:
            # Iterate the stream directly rather than collecting it into a list first -
            # short-circuits on the first failure instead of waiting for every batched
            # write to arrive before checking any of them.
            return all(r.is_ok for r in stream)
        finally:
            stream.close()

    return run_in_transaction(session, _op)


def get_related_customers(session: Session, customer_id: str) -> dict[str, int]:
    """Determines every customer related to ``customer_id`` - i.e. sharing ownership of
    at least one account - and how many accounts they share. Returns a map of related
    customer id to shared-account count."""
    stream = session.query(CUSTOMERS.id(customer_id)).bins(["accounts"]).execute()
    customer_row = stream.first()
    stream.close()
    if customer_row is None or not customer_row.is_ok or customer_row.record is None:
        return {}
    account_ids = customer_row.record.bins.get("accounts") or []
    if not account_ids:
        return {}

    account_keys = ACCOUNTS.ids(list(account_ids))
    counts: dict[str, int] = {}
    account_stream = session.query(account_keys).bins(["owners"]).execute()
    for row in account_stream:
        if row.is_ok and row.record is not None:
            for owner_id in row.record.bins.get("owners") or []:
                if owner_id != customer_id:
                    counts[owner_id] = counts.get(owner_id, 0) + 1
    account_stream.close()
    return counts


def get_related_account_ids(session: Session, customer_id: str) -> list[str] | None:
    """Gets the list of account ids related to a customer, or ``None`` if the customer
    does not exist."""
    stream = session.query(CUSTOMERS.id(customer_id)).execute()
    row = stream.first()
    stream.close()
    if row is None or not row.is_ok or row.record is None:
        return None
    return row.record.bins.get("accounts")


def remove_association(session: Session, customer_id: str, account_id: str) -> None:
    """Removes the association between a customer and an account, inside a transaction:
    removes the account id from the customer's ``accounts`` list, then the customer id
    from the account's ``owners`` list."""

    def _op(tx: Session) -> None:
        customer_stream = (
            tx.upsert(CUSTOMERS.id(customer_id))
            .bin("accounts").on_list_value(account_id).remove(return_type=ListReturnType.COUNT)
            .execute()
        )
        customer_row = customer_stream.first()
        customer_stream.close()
        removed_from_customer = bool(customer_row and customer_row.is_ok and customer_row.record
                                      and customer_row.record.bins.get("accounts", 0) > 0)
        if not removed_from_customer:
            raise RuntimeError(
                f"Customer record for key '{customer_id}', should contain account id "
                f"'{account_id}' in its accounts list, but does not",
            )

        account_stream = (
            tx.upsert(ACCOUNTS.id(account_id))
            .bin("owners").on_list_value(customer_id).remove(return_type=ListReturnType.COUNT)
            .execute()
        )
        account_row = account_stream.first()
        account_stream.close()
        removed_from_account = bool(account_row and account_row.is_ok and account_row.record
                                     and account_row.record.bins.get("owners", 0) > 0)
        if not removed_from_account:
            raise RuntimeError(
                f"Account record for key '{account_id}', should contain customer id "
                f"'{customer_id}' in its owners list, but does not",
            )

    run_in_transaction(session, _op)


def display_related_customers(relationships: dict[str, int]) -> None:
    """Prints the map of customer relationships to the console."""
    print(" Customer | Count")
    print("----------+------")
    for customer_id, count in relationships.items():
        print(f"{customer_id:>9} | {count:>3,}")


class ManyToManyRelationships(UseCase):
    def get_name(self) -> str:
        return "Many to many relationships"

    def get_description(self) -> str:
        return (
            "Demonstrate how to handle many-to-many relationships in Aerospike. Traversing "
            "relationships in both directions and adding entities are discussed."
        )

    def get_reference(self) -> str:
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/many-to-many-relationships.md"

    def setup(self, session: Session) -> None:
        session.truncate(ACCOUNTS)
        session.truncate(CUSTOMERS)

        print("Generating Customers")
        for i in range(1, NUM_CUSTOMERS + 1):
            customer = _random_customer(f"Cust-{i}")
            session.upsert(CUSTOMERS.id(customer.cust_id)).put(customer.to_bins()).execute()

        print("Generating Accounts")
        for _ in range(1, NUM_ACCOUNTS + 1):
            account = _random_account(str(uuid.uuid4()))
            num_owners = random.randint(1, 7)
            owner_ids = sorted({f"Cust-{random.randint(1, NUM_CUSTOMERS)}" for _ in range(num_owners)})
            add_account(session, account, owner_ids)

    def run(self, session: Session) -> None:
        result = get_related_customers(session, "Cust-1")
        print(f"\nFinding all the customers related to customer 'Cust-1' ({len(result)}):")
        display_related_customers(result)

        account_ids = get_related_account_ids(session, "Cust-1")
        if account_ids:
            print(f"\nRemoving association between customer 'Cust-1' and account '{account_ids[0]}'")
            remove_association(session, "Cust-1", account_ids[0])

            result = get_related_customers(session, "Cust-1")
            print(f"\nRelationships after association was removed ({len(result):,}):")
            display_related_customers(result)
