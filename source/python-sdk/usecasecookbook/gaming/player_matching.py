"""Port of ../../java-sdk's PlayerMatching (itself a port of ../../java's, see that
class's javadoc). Reuses :class:`~usecasecookbook.gaming.leaderboard.Leaderboard`'s
scoreboard update/query logic for scoring, and layers matchmaking on top via AEL
``where()`` filters for conditional (compare-and-swap style) writes.

Simpler than ../../java-sdk here: this SDK has no object mapper (every model is decoded
by hand via ``Player.from_bins``), and reading back a bin in the same call it was just
written returns a plain scalar, not a wrapper - so this port just reads back every bin it
needs, written or not, in one round trip.
"""

import random
from datetime import datetime, timezone

from aerospike_async import Key
from aerospike_async.exceptions import ResultCode
from aerospike_sdk.exceptions import AerospikeError
from aerospike_sdk.sync import Session

from usecasecookbook.async_util import Async
from usecasecookbook.gaming.leaderboard import PLAYERS, SCOREBOARD, Leaderboard
from usecasecookbook.gaming.model import Player
from usecasecookbook.txn import run_in_transaction
from usecasecookbook.use_case import UseCase

NUM_PLAYERS = 10_000
RUNTIME_SECS = 10
SHIELD_DURATION_MS = 5_000

FIRST_NAMES = ["Estefana", "Arnoldo", "Jacquelyne", "Harris", "Ardelia"]
LAST_NAMES = ["Ruecker", "MacGyver", "Willms", "Jones", "Renner"]


def _random_player(id: int) -> Player:
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    score = random.randint(0, 6200)
    return Player(
        id=id, user_name=f"{first.lower()}{last.lower()}", first_name=first, last_name=last,
        email=f"{first.lower()}.{last.lower()}@example.com",
        shield_expiry=0, online=False, being_attacked_by="", score=score,
    )


def calculate_new_elo_rating(player_rating: int, opponent_rating: int, score: int, k_factor: int) -> int:
    """Calculates the new Elo rating for a player after a game.

    Args:
        player_rating: The current rating of the player (e.g., 1600).
        opponent_rating: The rating of the opponent (e.g., 1800).
        score: The actual score of the game for the player: 1 = win, 0 = draw or loss.
        k_factor: The K-factor used to determine rating volatility (e.g., 20).

    Returns:
        The new rating of the player after the game, rounded to the nearest integer.
    """
    expected_score = 1.0 / (1.0 + pow(10.0, (opponent_rating - player_rating) / 400.0))
    new_rating = player_rating + k_factor * (score - expected_score)
    return round(new_rating)


class PlayerMatching(UseCase):
    def __init__(self) -> None:
        self.leaderboard = Leaderboard()

    def get_name(self) -> str:
        return "Player matching"

    def get_reference(self) -> str:
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/player-matching.md"

    def get_description(self) -> str:
        return (
            "Find players that match certain criteria at scale. Players play the game concurrently, but in order "
            "to attack they need to match against another player. The player cannot be online in this example, "
            "as they may be playing the game. In order to match, we want another player who:\n"
            "1. Is not online\n"
            "2. Who does not have a shield\n"
            "3. Who is not currently being attacked\n"
            "4. Whose score is >= 400\n"
            "5. Whose score is similar to this player\n"
            "A shield is given to a player when they have been attacked and defeated. These would normally be a decent length "
            "to stop them being attacked too often, but in this example will only be for 5s. Attackers defeat "
            "their opponents 80% of the time. If a player with a shield attacks during a shield, the shield is removed."
        )

    def setup(self, session: Session) -> None:
        session.truncate(PLAYERS)
        session.truncate(SCOREBOARD)

        print(f"Generating {NUM_PLAYERS:,} Players")
        for id in range(1, NUM_PLAYERS + 1):
            player = _random_player(id)
            session.upsert(PLAYERS.id(player.id)).put(player.to_bins()).execute()
            self.leaderboard.update_player_score(session, player.id, -1, player.score)

    def run(self, session: Session) -> None:
        self.test_eligibility(session)

        print("\nLet's play some games!")
        player1 = self.set_player_online(session, 1, True)
        print("Leader board before the games start...")
        self.leaderboard.show_players_around_player(session, player1.id, player1.score)

        counter = [0]

        def run_simulation(async_runner: Async) -> None:
            def attacker_loop() -> None:
                defender = self.find_player_to_attack(session, player1)
                if defender is not None:
                    self.play_game(session, player1, defender, True)
                    counter[0] += 1
            async_runner.periodic(0.1, attacker_loop)

            def random_player_loop() -> None:
                player_id = random.randint(2, NUM_PLAYERS)
                player = self.set_player_online(session, player_id, True)
                if player is not None:
                    try:
                        defender = self.find_player_to_attack(session, player)
                        if defender is not None:
                            self.play_game(session, player, defender, False)
                            counter[0] += 1
                    finally:
                        self.set_player_online(session, player_id, False)
            async_runner.periodic(0.005, random_player_loop, 20)

        Async.run_for(RUNTIME_SECS, run_simulation)

        print("Leader board after the games end...")
        self.leaderboard.show_players_around_player(session, player1.id, player1.score)

        print(f"Total battles played (all threads): {counter[0]:,}")
        self.set_player_online(session, player1.id, False)

    def find_player_to_attack_by_keys(
        self, session: Session, attacker_id: int, possibilities: list[Key],
    ) -> Player | None:
        """Finds a player to attack from a list of candidate keys: batch-reads them
        filtered down to eligible players, then tries each (randomly) with a filtered
        conditional write that claims them (sets ``beingAttackedBy``) atomically,
        retrying with another candidate if the claim fails because the candidate
        stopped being eligible in the meantime.
        """
        if not possibilities:
            return None
        filter_ael = self._player_filter()
        valid_records = []
        stream = session.query(possibilities).where(filter_ael).execute()
        for row in stream:
            if row.is_ok and row.record is not None:
                valid_records.append(row.record.bins)
        stream.close()

        while valid_records:
            record_num = random.randrange(len(valid_records))
            id = valid_records[record_num]["id"]

            row = (
                session.upsert(self.get_player_key(id))
                .where(filter_ael)
                .bin("beingAttackedBy").set_to(f"Player {id}")
                .bin("id").get()
                .bin("userName").get()
                .bin("firstName").get()
                .bin("lastName").get()
                .bin("email").get()
                .bin("shieldExpiry").get()
                .bin("online").get()
                .bin("beingAttackedBy").get()
                .bin("score").get()
                .execute().first()
            )

            if row is None or not row.is_ok or row.record is None:
                valid_records.pop(record_num)
            else:
                return Player.from_bins(row.record.bins)
        return None

    def find_player_to_attack(self, session: Session, attacker: Player) -> Player | None:
        """Given an attacker, finds a player of similar strength who is available to
        attack. The player will already have been locked for attacking.
        """
        similar_scores = self.leaderboard.get_scores_around_player(session, attacker.id, attacker.score, 20)
        keys = [self.get_player_key(e.id) for e in similar_scores if e.id != attacker.id]
        return self.find_player_to_attack_by_keys(session, attacker.id, keys)

    def play_game(self, session: Session, attacker: Player, defender: Player, show_battle: bool) -> None:
        """Plays a game between an attacker and a defender, adjusting scores, shield,
        and leaderboard.
        """
        if attacker is None or defender is None:
            return

        if show_battle:
            print(f"{attacker.user_name} is attacking {defender.user_name}... ", end="")

        original_attacker_score = attacker.score
        original_defender_score = defender.score

        probability_of_winning = 60 + (attacker.score - defender.score) // 5
        if random.randint(0, 100) >= probability_of_winning:
            attacker.score = calculate_new_elo_rating(attacker.score, defender.score, 1, 20)
            defender.score = calculate_new_elo_rating(defender.score, attacker.score, 0, 20)
            defender.shield_expiry = int(datetime.now(timezone.utc).timestamp() * 1000) + SHIELD_DURATION_MS
            if show_battle:
                print(
                    f"VICTORIOUS! (attacker: {original_attacker_score:,} -> {attacker.score:,}, "
                    f"defender: {original_defender_score:,}->{defender.score:,})"
                )
        else:
            attacker.score = calculate_new_elo_rating(attacker.score, defender.score, 0, 20)
            defender.score = calculate_new_elo_rating(defender.score, attacker.score, 1, 20)
            if show_battle:
                print(
                    f"REPELLED! (attacker: {original_attacker_score:,} -> {attacker.score:,}, "
                    f"defender: {original_defender_score:,}->{defender.score:,})"
                )
        attacker.shield_expiry = 0
        defender.being_attacked_by = None

        def op(tx: Session) -> None:
            self.leaderboard.update_player_score(tx, attacker.id, original_attacker_score, attacker.score)
            self.leaderboard.update_player_score(tx, defender.id, original_defender_score, defender.score)

            (
                tx.upsert(self.get_player_key(attacker.id))
                .bin("score").set_to(attacker.score)
                .bin("shieldExpiry").set_to(attacker.shield_expiry)
                .execute()
            )
            (
                tx.upsert(self.get_player_key(defender.id))
                .bin("score").set_to(defender.score)
                .bin("shieldExpiry").set_to(defender.shield_expiry)
                .bin("beingAttackedBy").set_to("")
                .execute()
            )

        run_in_transaction(session, op)

    def _player_filter(self) -> str:
        """Every ``Player`` always has ``beingAttackedBy`` set at creation, so (unlike
        the legacy filter) this doesn't need a missing-bin fallback clause.
        """
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        return f"$.online == false and $.shieldExpiry < {now_ms} and $.beingAttackedBy == '' and $.score > 400"

    def get_player_key(self, id: int) -> Key:
        return PLAYERS.id(id)

    def set_player_online(self, session: Session, player_id: int, is_online: bool) -> Player | None:
        """Sets a player online and returns their details, or ``None`` if ``is_online``
        is true and the player was already online.
        """
        builder = session.upsert(self.get_player_key(player_id))
        if is_online:
            builder = builder.where("$.online == false")
        row = (
            builder
            .bin("online").set_to(is_online)
            .bin("id").get()
            .bin("userName").get()
            .bin("firstName").get()
            .bin("lastName").get()
            .bin("email").get()
            .bin("shieldExpiry").get()
            .bin("online").get()
            .bin("beingAttackedBy").get()
            .bin("score").get()
            .execute().first()
        )
        if row is None or not row.is_ok or row.record is None:
            return None
        return Player.from_bins(row.record.bins)

    def reset_player_to(self, session: Session, player_id: int, score: int) -> None:
        """Resets a player to offline, no shield, not being attacked. Sets the score
        too if ``score >= 0``.
        """
        builder = (
            session.upsert(self.get_player_key(player_id))
            .bin("online").set_to(False)
            .bin("shieldExpiry").set_to(0)
            .bin("beingAttackedBy").set_to("")
        )
        if score >= 0:
            builder = builder.bin("score").set_to(score)
        builder.execute()

    def can_attack_player_test(self, session: Session, player_id: int) -> bool:
        """Determines if the player can currently be attacked. Not for real game play -
        the result could be stale by the time it's returned.
        """
        try:
            row = (
                session.query(self.get_player_key(player_id))
                .where(self._player_filter())
                .fail_on_filtered_out()
                .execute().first()
            )
            if row is None:
                print(f"*** Key {player_id} does not exist!")
                return False
            return True
        except AerospikeError as e:
            if e.result_code == ResultCode.FILTERED_OUT:
                return False
            raise

    def test_eligibility(self, session: Session) -> int:
        player_id = 1
        row = session.query(self.get_player_key(player_id)).execute().first()
        if row is None or row.record is None:
            raise RuntimeError(
                f"No player with id {player_id} found - run setup() before run()/test_eligibility()."
            )
        original_score = row.record.bins["score"]
        print(f"\nTesting eligibility for player {player_id}")
        self.reset_player_to(session, player_id, 590)
        passed = self.can_attack_player_test(session, player_id)
        print(f"- Checking player can validly be attacked: {'PASSED' if passed else 'FAILED'}")

        self.set_player_online(session, player_id, True)
        passed = not self.can_attack_player_test(session, player_id)
        print(f"- Checking player cannot be attacked when online: {'PASSED' if passed else 'FAILED'}")

        self.reset_player_to(session, player_id, 50)
        passed = not self.can_attack_player_test(session, player_id)
        print(f"- Checking player cannot be attacked with a low score: {'PASSED' if passed else 'FAILED'}")

        self.reset_player_to(session, player_id, 500)
        self.leaderboard.update_player_score(session, player_id, original_score, 1234)
        return 1234
