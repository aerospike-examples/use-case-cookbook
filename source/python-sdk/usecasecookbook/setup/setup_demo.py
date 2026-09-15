"""Port of the legacy SetupDemo (see ../../java and ../../java-sdk). The legacy versions
seed data with the Java Object Generator / Java Object Mapper; here accounts are hand-built
with the standard library ``random`` module and converted to/from bins manually (see
setup/model.py) since this SDK has no object mapper.
"""

import random
import uuid
from datetime import datetime, timedelta, timezone

from aerospike_sdk import DataSet, SyncSession

from usecasecookbook import config
from usecasecookbook.setup.model import Account
from usecasecookbook.use_case import UseCase

NUM_ACCOUNTS = 1_000
FIRST_NAMES = ["Estefana", "Arnoldo", "Jacquelyne", "Harris", "Ardelia"]
LAST_NAMES = ["Ruecker", "MacGyver", "Willms", "Jones", "Renner"]

ACCOUNTS = DataSet.of(config.NAMESPACE, "uccb_accounts")


def _random_account() -> Account:
    name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}'s account"
    balance_in_cents = random.randint(500, 2_000_000)
    five_years = timedelta(days=5 * 365)
    date_opened = datetime.now(timezone.utc) - timedelta(seconds=random.uniform(0, five_years.total_seconds()))
    return Account(str(uuid.uuid4()), name, balance_in_cents, date_opened)


class SetupDemo(UseCase):
    def get_name(self) -> str:
        return "Demo setup"

    def get_description(self) -> str:
        return (
            "First application to make sure your environment is set up correctly. "
            "Inserts some Accounts and reads the data back"
        )

    def get_reference(self) -> str:
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/setup.md"

    def setup(self, session: SyncSession) -> None:
        session.truncate(ACCOUNTS)

        print(f"Generating {NUM_ACCOUNTS:,} accounts...")
        for _ in range(NUM_ACCOUNTS):
            account = _random_account()
            session.upsert(ACCOUNTS.id(account.id)).put(account.to_bins()).execute()
        print("Setup complete!")

    def run(self, session: SyncSession) -> None:
        print("Query first 100 accounts")
        stream = session.query(ACCOUNTS).limit(100).execute()
        for row in stream:
            if row.is_ok and row.record is not None:
                account = Account.from_bins(row.record.bins)
                print(
                    f"Id: {account.id}, Account Name: {account.account_name}, "
                    f"Balance ${account.balance_in_cents / 100:.2f}, Date Opened: {account.date_opened}"
                )
        stream.close()
