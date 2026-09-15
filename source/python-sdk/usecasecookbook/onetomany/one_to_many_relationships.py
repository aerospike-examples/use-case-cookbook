"""Port of the legacy OneToManyRelationships (see ../../java and ../../java-sdk).
Demonstrates a one-to-many relationship: each agent holds a set-like list of listing
ids, and each listing stores the id of its owning agent, so the relationship can be
queried from either side.

Uses ``usecasecookbook.txn.run_in_transaction`` instead of raw ``session.do_in_transaction``
(see that module - it degrades to a plain, non-atomic call on this dev cluster, which has
no strong-consistency namespace).
"""

import random
from datetime import datetime, timedelta
from typing import List

from aerospike_async import ListReturnType
from aerospike_sdk import DataSet, SyncSession

from usecasecookbook import config
from usecasecookbook.onetomany.model import Agent, Listing
from usecasecookbook.txn import run_in_transaction
from usecasecookbook.use_case import UseCase

NUM_AGENTS = 1_000
NUM_LISTINGS = 5_000

FIRST_NAMES = ["Estefana", "Arnoldo", "Jacquelyne", "Harris", "Ardelia"]
LAST_NAMES = ["Ruecker", "MacGyver", "Willms", "Jones", "Renner"]
STREETS = ["Main St", "Oak Ave", "Elm St", "Maple Dr", "Cedar Ln"]
CITIES = ["Springfield", "Franklin", "Greenville", "Clinton", "Fairview"]
STATES = ["CA", "TX", "NY", "FL", "WA"]

AGENTS = DataSet.of(config.NAMESPACE, "uccb_agent")
LISTINGS = DataSet.of(config.NAMESPACE, "uccb_listing")


def _random_agent(agent_id: int) -> Agent:
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    ten_years = timedelta(days=10 * 365)
    reg_date = datetime.now() - timedelta(seconds=random.uniform(0, ten_years.total_seconds()))
    return Agent(
        agent_id, first, last,
        f"{first.lower()}.{last.lower()}@example.com",
        f"555-{random.randint(0, 9999):04d}",
        reg_date,
    )


def _random_listing(listing_id: str) -> Listing:
    one_year = timedelta(days=365)
    date_listed = datetime.now() - timedelta(seconds=random.uniform(0, one_year.total_seconds()))
    return Listing(
        listing_id,
        f"{random.randint(100, 9999)} {random.choice(STREETS)}",
        "",
        random.choice(CITIES),
        random.choice(STATES),
        f"{random.randint(0, 99999):05d}",
        f"https://example.com/listings/{listing_id}",
        date_listed,
        0,
        "A lovely property in a great neighborhood.",
    )


def _add_listing_to_agent(session: SyncSession, listing_id: str, agent_id: int) -> None:
    def _op(tx: SyncSession) -> None:
        tx.upsert(AGENTS.id(agent_id)).bin("listings").list_append_items(
            [listing_id], unique=True, no_fail=True,
        ).execute()
        tx.upsert(LISTINGS.id(listing_id)).bin("agentId").set_to(agent_id).execute()

    run_in_transaction(session, _op)


def add_listing(session: SyncSession, agent_id: int, listing: Listing) -> None:
    """Saves ``listing`` and appends its id to ``agent_id``'s ``listings`` list, in a
    transaction."""
    listing.agent_id = agent_id

    def _op(tx: SyncSession) -> None:
        tx.upsert(LISTINGS.id(listing.id)).put(listing.to_bins()).execute()
        tx.upsert(AGENTS.id(agent_id)).bin("listings").list_append_items(
            [listing.id], unique=True, no_fail=True,
        ).execute()

    run_in_transaction(session, _op)


def delete_listing(session: SyncSession, listing_id: str) -> bool:
    """Deletes a listing and removes it from its agent's ``listings`` list, in a
    transaction. Returns ``False`` if the listing did not exist."""
    listing_key = LISTINGS.id(listing_id)

    def _op(tx: SyncSession) -> bool:
        stream = tx.query(listing_key).bins(["agentId"]).execute()
        row = stream.first()
        stream.close()
        if row is None or not row.is_ok or row.record is None:
            return False
        agent_id = row.record.bins["agentId"]
        tx.delete(listing_key).execute()

        remove_stream = (
            tx.upsert(AGENTS.id(agent_id))
            .bin("listings").on_list_value(listing_id).remove(return_type=ListReturnType.COUNT)
            .execute()
        )
        remove_row = remove_stream.first()
        remove_stream.close()
        count = remove_row.record.bins["listings"] if remove_row and remove_row.is_ok else 0
        return count > 0

    return run_in_transaction(session, _op)


def get_listings(session: SyncSession, agent_id: int) -> List[Listing]:
    """Reads the agent's ``listings`` id list, then batch-reads every listing it
    references. Returns an empty list if the agent doesn't exist or has none."""
    agent_key = AGENTS.id(agent_id)

    def _op(tx: SyncSession) -> List[Listing]:
        stream = tx.query(agent_key).bins(["listings"]).execute()
        row = stream.first()
        stream.close()
        if row is None or not row.is_ok or row.record is None:
            return []
        listing_ids = row.record.bins.get("listings") or []
        if not listing_ids:
            return []

        keys = LISTINGS.ids(list(listing_ids))
        results: List[Listing] = []
        listing_stream = tx.query(keys).execute()
        for r in listing_stream:
            if r.is_ok and r.record is not None:
                results.append(Listing.from_bins(r.record.bins))
        listing_stream.close()
        return results

    return run_in_transaction(session, _op)


class OneToManyRelationships(UseCase):
    def get_name(self) -> str:
        return "One to many relationships"

    def get_description(self) -> str:
        return (
            "Demonstrate how to handle one-to-many relationships in Aerospike. Both being able to "
            "query only from the parent to the child, and being able to query from the child to the "
            "parent as well, are discussed."
        )

    def get_reference(self) -> str:
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/one-to-many-relationships.md"

    def setup(self, session: SyncSession) -> None:
        session.truncate(AGENTS)
        session.truncate(LISTINGS)

        print("Generating Agents")
        for agent_id in range(1, NUM_AGENTS + 1):
            session.upsert(AGENTS.id(agent_id)).put(_random_agent(agent_id).to_bins()).execute()

        print("Generating Listings")
        for i in range(1, NUM_LISTINGS + 1):
            listing_id = f"Listing-{i}"
            session.upsert(LISTINGS.id(listing_id)).put(_random_listing(listing_id).to_bins()).execute()

        print("Associating listings with agents")
        for i in range(1, NUM_LISTINGS + 1):
            listing_id = f"Listing-{i}"
            agent_id = random.randint(1, NUM_AGENTS)
            _add_listing_to_agent(session, listing_id, agent_id)

    def run(self, session: SyncSession) -> None:
        agent_id = random.randint(1, NUM_AGENTS)
        print(f"Examining listings for agent {agent_id}:")
        listings = get_listings(session, agent_id)
        print(f"\nCurrent listings ({len(listings):,}):")
        for listing in listings:
            print(f"   {listing}")

        new_listing = _random_listing("Listing-X999")
        print(f"\nAdding a new listing ({new_listing.id})")
        add_listing(session, agent_id, new_listing)

        listings = get_listings(session, agent_id)
        print(f"Listings after adding the new listing ({len(listings):,}):")
        for listing in listings:
            print(f"   {listing}")

        deleted = delete_listing(session, listings[0].id)
        print(f"\nDeleting Listing {listings[0].id}: {deleted}")

        listings = get_listings(session, agent_id)
        print(f"Listings after deleting a listing ({len(listings):,}):")
        for listing in listings:
            print(f"   {listing}")
