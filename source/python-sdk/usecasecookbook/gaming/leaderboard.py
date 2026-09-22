"""Port of ../../java-sdk's Leaderboard (itself a port of ../../java's, see that class's
javadoc). A scoreboard is stored as a key-ordered map per score "bucket"
(``score // SCORES_PER_BUCKET``), where each map key is a zero-padded
``"score-playerId"`` composite string (so map key order == score order) and each map
value is the player id.

Reading the players around a given score/player uses ``on_map_key_relative_index_range`` to
fetch a clamped index range either side of the player's map key in one round trip - it clamps at
the bucket's boundaries and, when the anchor key is stale (the player's score changed
concurrently), falls back to the range around where that key would sort. Overflow past the
current bucket's boundary is then resolved by reading extra keys from neighboring buckets via
``on_map_index_range``, mirroring ../../java's addOverflow*PlayersIfNeeded.
"""

import random
import threading
from bisect import bisect_left

from aerospike_async import Key, MapOrder
from aerospike_sdk import DataSet
from aerospike_sdk.sync import Session

from usecasecookbook import config
from usecasecookbook.async_util import Async
from usecasecookbook.gaming.model import Player
from usecasecookbook.txn import run_in_transaction
from usecasecookbook.use_case import UseCase

SCOREBOARD_BIN = "score"
SCORES_PER_BUCKET = 25
NUM_PLAYERS = 100_000
MAX_SCORE = 6300
MAX_BUCKETS = MAX_SCORE // SCORES_PER_BUCKET

NUM_THREADS = 50
PLAYER1_UPDATE_PERIOD = 0.05
THREAD_UPDATE_PERIOD = 0.005
RUNTIME_SECS = 20
SCOREBOARD_DISPLAY_PERIOD = 3

FIRST_NAMES = ["Estefana", "Arnoldo", "Jacquelyne", "Harris", "Ardelia"]
LAST_NAMES = ["Ruecker", "MacGyver", "Willms", "Jones", "Renner"]

PLAYERS = DataSet.of(config.NAMESPACE, "uccb_player")
SCOREBOARD = DataSet.of(config.NAMESPACE, "scoreboard")


class ScoreEntry:
    """A player id/score pair decoded from a scoreboard map key - just enough to look
    the player back up; see :meth:`Leaderboard.populate_full_player_details`.
    """

    __slots__ = ("id", "score")

    def __init__(self, id: int, score: int):
        self.id = id
        self.score = score


class Leaderboard(UseCase):
    def get_name(self) -> str:
        return "Gaming Leaderboard"

    def get_description(self) -> str:
        return (
            "Demonstrate an approach to create a leaderboard in Aerospike. When a player achieves a score in "
            "a game the leaderboard should be updated to reflect their score. The leaderboard around a "
            "particular score can be shown. When running, one player (id = 1) will be focused on, changing "
            f"their score every {PLAYER1_UPDATE_PERIOD * 1000:,.0f} milliseconds, and showing the scoreboard "
            f"around their score every {SCOREBOARD_DISPLAY_PERIOD} seconds. "
            f"Additionally there are {NUM_THREADS:,} background threads which are randomly updating other "
            f"scores every {THREAD_UPDATE_PERIOD * 1000:,.0f}ms per thread."
        )

    def get_reference(self) -> str:
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/leaderboard.md"

    @staticmethod
    def _random_player(id: int) -> Player:
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        score = random.randint(0, 6200)
        return Player(
            id=id, user_name=f"{first.lower()}{last.lower()}", first_name=first, last_name=last,
            email=f"{first.lower()}.{last.lower()}@example.com",
            shield_expiry=0, online=False, being_attacked_by="", score=score,
        )

    def setup(self, session: Session) -> None:
        session.truncate(PLAYERS)
        session.truncate(SCOREBOARD)

        print(f"Generating {NUM_PLAYERS:,} Players")
        for id in range(1, NUM_PLAYERS + 1):
            player = self._random_player(id)
            session.upsert(PLAYERS.id(player.id)).put(player.to_bins()).execute()
            self.update_player_score(session, player.id, -1, player.score)

    def run(self, session: Session) -> None:
        row = session.query(PLAYERS.id(1)).execute().first()
        score = row.record.bins["score"]
        player_id = row.record.bins["id"]
        self.show_players_around_player(session, player_id, score)

        print(f"\nStarting gaming for {RUNTIME_SECS}s...")
        score_val = [score]
        lock = threading.Lock()

        def run_simulation(async_runner: Async) -> None:
            def show() -> None:
                with lock:
                    self.show_players_around_player(session, player_id, score_val[0])
            async_runner.periodic(SCOREBOARD_DISPLAY_PERIOD, show)

            def update_player1() -> None:
                with lock:
                    current_score = score_val[0]
                    new_score = self._change_score(current_score, 1)
                    self.update_player_score(session, player_id, current_score, new_score)
                    score_val[0] = new_score
            async_runner.periodic(PLAYER1_UPDATE_PERIOD, update_player1)

            def update_random_player() -> None:
                player_id_to_change = random.randint(2, NUM_PLAYERS)
                row = session.query(PLAYERS.id(player_id_to_change)).execute().first()
                if row is not None and row.is_ok and row.record is not None:
                    current_score = row.record.bins["score"]
                    self.update_player_score(
                        session, player_id_to_change, current_score, self._change_score(current_score),
                    )
            async_runner.periodic(THREAD_UPDATE_PERIOD, update_random_player, NUM_THREADS)

        Async.run_for(RUNTIME_SECS, run_simulation)

        print("\n************************")
        print("*** Final scoreboard ***")
        print("************************")
        self.show_players_around_player(session, player_id, score_val[0])

    @staticmethod
    def _change_score(current_score: int, offset: int = 0) -> int:
        score_change = random.randint(0, 48) - 24 + offset
        return max(0, min(6200, current_score) + score_change)

    @staticmethod
    def _determine_bucket_for_score(score: int) -> int:
        return score // SCORES_PER_BUCKET

    @staticmethod
    def _scoreboard_key(score: int) -> Key:
        return SCOREBOARD.id(Leaderboard._determine_bucket_for_score(score))

    @staticmethod
    def _scoreboard_key_for_bucket(bucket: int) -> Key:
        return SCOREBOARD.id(bucket)

    @staticmethod
    def _map_key(player_id: int, score: int) -> str:
        return f"{score:05d}-{player_id:09d}"

    @staticmethod
    def _map_key_to_score_entry(map_key: str) -> ScoreEntry:
        score_str, id_str = map_key.split("-")
        return ScoreEntry(id=int(id_str), score=int(score_str))

    def update_player_score(self, session: Session, player_id: int, old_score: int, new_score: int) -> None:
        """Sets the score of the player, both on the player record and the scoreboard's map."""

        def op(tx: Session) -> None:
            new_bucket_key = self._scoreboard_key(new_score)
            if old_score < 0:
                map_key = self._map_key(player_id, new_score)
                (
                    tx.upsert(new_bucket_key)
                    .bin(SCOREBOARD_BIN).on_map_key(map_key, create_type=MapOrder.KEY_ORDERED).set_to(player_id)
                    .execute()
                )
            else:
                new_map_key = self._map_key(player_id, new_score)
                old_map_key = self._map_key(player_id, old_score)

                if self._determine_bucket_for_score(new_score) == self._determine_bucket_for_score(old_score):
                    (
                        tx.upsert(new_bucket_key)
                        .bin(SCOREBOARD_BIN).on_map_key(old_map_key).remove()
                        .bin(SCOREBOARD_BIN).on_map_key(new_map_key, create_type=MapOrder.KEY_ORDERED).set_to(player_id)
                        .execute()
                    )
                else:
                    old_bucket_key = self._scoreboard_key(old_score)
                    tx.upsert(old_bucket_key).bin(SCOREBOARD_BIN).on_map_key(old_map_key).remove().execute()
                    (
                        tx.upsert(new_bucket_key)
                        .bin(SCOREBOARD_BIN).on_map_key(new_map_key, create_type=MapOrder.KEY_ORDERED).set_to(player_id)
                        .execute()
                    )

            if old_score >= 0:
                tx.upsert(PLAYERS.id(player_id)).bin("score").set_to(new_score).execute()

        run_in_transaction(session, op)

    def _add_overflow_lower(self, session: Session, lower: list[str], bucket: int, n: int) -> None:
        current = bucket - 1
        while current >= 0 and len(lower) < n:
            index = len(lower) - n
            row = (
                session.query(self._scoreboard_key_for_bucket(current))
                .bin(SCOREBOARD_BIN).on_map_index_range(index).get_keys()
                .execute().first()
            )
            if row is not None and row.is_ok and row.record is not None:
                extra = row.record.bins.get(SCOREBOARD_BIN) or []
                lower[0:0] = extra
            current -= 1

    def _add_overflow_higher(self, session: Session, higher: list[str], bucket: int, n: int) -> None:
        current = bucket + 1
        while current <= MAX_BUCKETS and len(higher) < n + 1:
            count = n + 1 - len(higher)
            row = (
                session.query(self._scoreboard_key_for_bucket(current))
                .bin(SCOREBOARD_BIN).on_map_index_range(0, count).get_keys()
                .execute().first()
            )
            if row is not None and row.is_ok and row.record is not None:
                extra = row.record.bins.get(SCOREBOARD_BIN) or []
                higher.extend(extra)
            current += 1

    def get_scores_around_player(
        self, session: Session, player_id: int, score: int, num_players_either_side: int,
    ) -> list[ScoreEntry]:
        """Gets the scores on either side of a player's score (see module docstring). Returns
        entries with just ``id``/``score`` populated - see :meth:`populate_full_player_details`
        for the rest.
        """
        map_key = self._map_key(player_id, score)
        bucket = self._determine_bucket_for_score(score)

        row = (
            session.query(self._scoreboard_key(score))
            .bin(SCOREBOARD_BIN)
            .on_map_key_relative_index_range(map_key, -num_players_either_side, 2 * num_players_either_side + 1)
            .get_keys()
            .execute().first()
        )
        combined: list[str] = []
        if row is not None and row.is_ok and row.record is not None:
            combined = list(row.record.bins.get(SCOREBOARD_BIN) or [])

        # map_key may be stale (the player's score changed concurrently) and absent from
        # combined - bisect_left gives the correct split point either way, since combined is
        # lexically sorted regardless of whether map_key is actually present in it.
        pos = bisect_left(combined, map_key)
        lower = combined[:pos]
        higher = combined[pos:]

        self._add_overflow_lower(session, lower, bucket, num_players_either_side)
        self._add_overflow_higher(session, higher, bucket, num_players_either_side)

        return [self._map_key_to_score_entry(k) for k in lower] + [self._map_key_to_score_entry(k) for k in higher]

    def populate_full_player_details(self, session: Session, partial_players: list[ScoreEntry]) -> list[Player]:
        if not partial_players:
            return []
        keys = [PLAYERS.id(p.id) for p in partial_players]
        stream = session.query(keys).execute()
        players = []
        for row in stream:
            if row.is_ok and row.record is not None:
                players.append(Player.from_bins(row.record.bins))
        stream.close()
        return players

    def show_players_around_player(self, session: Session, player_id: int, score: int) -> None:
        player_list = self.populate_full_player_details(
            session, self.get_scores_around_player(session, player_id, score, 6),
        )
        print(f"\nCurrent scoreboard around player {player_id}")
        print("Score |                     User Name                      |  Id ")
        print("-----------------------------------------------------------------")
        for this_player in player_list:
            bold = this_player.id == player_id
            if bold:
                print("\033[1m", end="")
            print(f"{this_player.score:>5} | {this_player.user_name:>50} | {this_player.id}")
            if bold:
                print("\033[0m", end="")
