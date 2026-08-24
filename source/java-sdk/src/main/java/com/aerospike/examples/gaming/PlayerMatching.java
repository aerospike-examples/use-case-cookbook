package com.aerospike.examples.gaming;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ChainableOperationBuilder;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.TypedDataSet;
import com.aerospike.client.sdk.TypedKey;
import com.aerospike.client.sdk.TypedKeyList;
import com.aerospike.examples.Async;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.gaming.model.Player;

/**
 * SDK port of the legacy {@code PlayerMatching} (see ../../java). Reuses {@link Leaderboard}'s
 * scoreboard update/query logic for scoring, and layers matchmaking on top via filter expressions
 * ({@code where(Exp)}) instead of the legacy client's {@code Policy.filterExp}.
 * <p/>
 * The legacy version's {@code setPlayerOnline} builds its conditional-write filter against a
 * bin called {@code "isOnline"}, which is never written anywhere (the actual online-state bin is
 * {@code "online"}) - so that filter can never match. This port fixes it to check {@code "online"},
 * matching the method's own documented intent (refuse to re-mark an already-online player online).
 */
public class PlayerMatching implements UseCase {

    private static final int NUM_PLAYERS = 10_000;
    private static final int RUNTIME_SECS = 10;
    private static final long SHIELD_DURATION_MS = TimeUnit.SECONDS.toMillis(5);

    private static final String[] FIRST_NAMES = {"Estefana", "Arnoldo", "Jacquelyne", "Harris", "Ardelia"};
    private static final String[] LAST_NAMES = {"Ruecker", "MacGyver", "Willms", "Jones", "Renner"};

    private final Leaderboard leaderboard = new Leaderboard();

    @Override
    public String getName() {
        return "Player matching";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/player-matching.md";
    }

    @Override
    public String getDescription() {
        return "Find players that match certain criteria at scale. Players play the game concurrently, but in order "
                + "to attack they need to match against another player. The player cannot be online in this example, "
                + "as they may be playing the game. In order to match, we want another player who:\n"
                + "1. Is not online\n"
                + "2. Who does not have a shield\n"
                + "3. Who is not currently being attacked\n"
                + "4. Whose score is >= 400\n"
                + "5. Whose score is similar to this player\n"
                + "A shield is given to a player when they have been attacked and defeated. These would normally be a decent length "
                + "to stop them being attacked too often, but in this example will only be for 5s. Attackers defeat "
                + "their opponents 80% of the time. If a player with a shield attacks during a shield, the shield is removed.";
    }

    private final TypedDataSet<Player> players =
            TypedDataSet.of(System.getProperty("demo.namespace", "test"), "uccb_player", Player.class);
    private final DataSet scoreboard = DataSet.of(System.getProperty("demo.namespace", "test"), "scoreboard");

    private Player randomPlayer(int id) {
        String first = FIRST_NAMES[ThreadLocalRandom.current().nextInt(FIRST_NAMES.length)];
        String last = LAST_NAMES[ThreadLocalRandom.current().nextInt(LAST_NAMES.length)];
        int score = ThreadLocalRandom.current().nextInt(0, 6201);
        return new Player(id, first.toLowerCase() + last.toLowerCase(), first, last,
                first.toLowerCase() + "." + last.toLowerCase() + "@example.com",
                0, false, "", score);
    }

    @Override
    public void setup(Session session) throws Exception {
        session.truncate(players);
        session.truncate(scoreboard);

        System.out.printf("Generating %,d Players%n", NUM_PLAYERS);
        for (int id = 1; id <= NUM_PLAYERS; id++) {
            Player player = randomPlayer(id);
            session.upsert(players).object(player).execute();
            leaderboard.updatePlayerScore(session, player.getId(), -1, player.getScore());
        }
    }

    @Override
    public void run(Session session) throws Exception {
        testEligibility(session);

        System.out.printf("%nLet's play some games!%n");
        Player player1 = setPlayerOnline(session, 1, true);
        System.out.println("Leader board before the games start...");
        leaderboard.showPlayersAroundPlayer(session, player1.getId(), player1.getScore());

        AtomicInteger counter = new AtomicInteger();
        Async.runFor(Duration.ofSeconds(RUNTIME_SECS), (async) -> {
            async.periodic(Duration.ofMillis(100), () -> {
                findPlayerToAttack(session, player1).ifPresent(defender -> {
                    playGame(session, player1, defender, true);
                    counter.incrementAndGet();
                });
            });

            async.periodic(Duration.ofMillis(5), 20, () -> {
                int playerId = async.rand().nextInt(NUM_PLAYERS - 1) + 2;
                Player player = setPlayerOnline(session, playerId, true);
                if (player != null) {
                    try {
                        findPlayerToAttack(session, player).ifPresent(defender -> {
                            playGame(session, player, defender, false);
                            counter.incrementAndGet();
                        });
                    }
                    finally {
                        setPlayerOnline(session, playerId, false);
                    }
                }
            });
        });
        System.out.println("Leader board after the games end...");
        leaderboard.showPlayersAroundPlayer(session, player1.getId(), player1.getScore());

        System.out.printf("Total battles played (all threads): %,d%n", counter.get());
        setPlayerOnline(session, player1.getId(), false);
    }

    /**
     * Finds a player to attack from a list of candidate keys: batch-reads them filtered down to
     * eligible players, then tries each (randomly) with a filtered conditional write that claims
     * them (sets {@code beingAttackedBy}) atomically, retrying with another candidate if the claim
     * fails because the candidate stopped being eligible in the meantime.
     */
    public Optional<Player> findPlayerToAttack(Session session, int attackerId, TypedKeyList<Player> possibilities) {
        String filter = getPlayerFilter();
        List<Record> validRecords = new ArrayList<>();
        try (var stream = session.query(possibilities).where(filter).execute()) {
            stream.forEach(result -> {
                if (result.isOk()) {
                    validRecords.add(result.recordOrThrow());
                }
            });
        }

        var mapper = session.getRecordMappingFactory().getMapper(Player.class);
        while (!validRecords.isEmpty()) {
            int recordNum = ThreadLocalRandom.current().nextInt(validRecords.size());
            int id = validRecords.get(recordNum).getInt("id");
            // Note: "beingAttackedBy" is set above but deliberately not also read back below -
            // requesting both a write and a read of the same bin in one operation returns a
            // multi-result wrapper for that bin instead of a plain value, so we just set it
            // directly on the Player below from the value we already know we wrote.
            Optional<Player> player = session.upsert(getPlayerKey(id))
                    .where(filter)
                    .bin("beingAttackedBy").setTo("Player " + id)
                    .bin("id").get()
                    .bin("userName").get()
                    .bin("firstName").get()
                    .bin("lastName").get()
                    .bin("email").get()
                    .bin("shieldExpiry").get()
                    .bin("online").get()
                    .bin("score").get()
                    .execute().getFirst(mapper);

            if (player.isEmpty()) {
                validRecords.remove(recordNum);
            }
            else {
                player.get().setBeingAttackedBy("Player " + id);
                return player;
            }
        }
        return Optional.empty();
    }

    /** Given an attacker, finds a player of similar strength who is available to attack. */
    public Optional<Player> findPlayerToAttack(Session session, Player attacker) {
        List<Player> similarScores = leaderboard.getScoresAroundPlayer(session, attacker.getId(), attacker.getScore(), 20);
        TypedKeyList<Player> keys = new TypedKeyList<>();
        similarScores.stream()
                .filter(player -> player.getId() != attacker.getId())
                .forEach(player -> keys.add(getPlayerKey(player.getId())));
        return findPlayerToAttack(session, attacker.getId(), keys);
    }

    public static int calculateNewEloRating(int playerRating, int opponentRating, int score, int kFactor) {
        double expectedScore = 1.0 / (1.0 + Math.pow(10.0, (opponentRating - playerRating) / 400.0));
        double newRating = playerRating + kFactor * (score - expectedScore);
        return (int) Math.round(newRating);
    }

    /** Plays a game between an attacker and a defender, adjusting scores, shield, and leaderboard. */
    public void playGame(Session session, Player attacker, Player defender, boolean showBattle) {
        if (attacker == null || defender == null) {
            return;
        }

        if (showBattle) {
            System.out.printf("%s is attacking %s... ", attacker.getUserName(), defender.getUserName());
        }
        try {
            Thread.sleep(5);
        }
        catch (InterruptedException ignored) {
        }
        int originalAttackerScore = attacker.getScore();
        int originalDefenderScore = defender.getScore();

        int probabilityOfWinning = 60 + (attacker.getScore() - defender.getScore()) / 5;
        if (ThreadLocalRandom.current().nextInt(101) >= probabilityOfWinning) {
            attacker.setScore(calculateNewEloRating(attacker.getScore(), defender.getScore(), 1, 20));
            defender.setScore(calculateNewEloRating(defender.getScore(), attacker.getScore(), 0, 20));
            defender.setShieldExpiry(new Date().getTime() + SHIELD_DURATION_MS);
            if (showBattle) {
                System.out.printf("VICTORIOUS! (attacker: %,d -> %,d, defender: %,d->%,d)%n",
                        originalAttackerScore, attacker.getScore(),
                        originalDefenderScore, defender.getScore());
            }
        }
        else {
            attacker.setScore(calculateNewEloRating(attacker.getScore(), defender.getScore(), 0, 20));
            defender.setScore(calculateNewEloRating(defender.getScore(), attacker.getScore(), 1, 20));
            if (showBattle) {
                System.out.printf("REPELLED! (attacker: %,d -> %,d, defender: %,d->%,d)%n",
                        originalAttackerScore, attacker.getScore(),
                        originalDefenderScore, defender.getScore());
            }
        }
        attacker.setShieldExpiry(0);
        defender.setBeingAttackedBy(null);

        session.doInTransaction(tx -> {
            leaderboard.updatePlayerScore(tx, attacker.getId(), originalAttackerScore, attacker.getScore());
            leaderboard.updatePlayerScore(tx, defender.getId(), originalDefenderScore, defender.getScore());

            tx.upsert(getPlayerKey(attacker.getId()))
                    .bin("score").setTo(attacker.getScore())
                    .bin("shieldExpiry").setTo(attacker.getShieldExpiry())
                    .execute();

            tx.upsert(getPlayerKey(defender.getId()))
                    .bin("score").setTo(defender.getScore())
                    .bin("shieldExpiry").setTo(defender.getShieldExpiry())
                    .bin("beingAttackedBy").setTo("")
                    .execute();
        });
    }

    /**
     * AEL translation of the legacy filter, verified against a live cluster to return identical
     * candidate counts to the original {@code Exp} form. One deliberate simplification: the
     * original also treated a missing {@code beingAttackedBy} bin as eligible ({@code Exp.or(...,
     * Exp.not(Exp.binExists(...)))}) - that fallback is unreachable in this use case's data model
     * (every {@code Player} always has the bin, set at creation) and AEL's bin-existence syntax
     * couldn't be verified against real missing-bin data here, so it's dropped rather than guessed.
     */
    private String getPlayerFilter() {
        return String.format(
                "$.online == false and $.shieldExpiry < %d and $.beingAttackedBy == '' and $.score > 400",
                new Date().getTime());
    }

    public TypedKey<Player> getPlayerKey(int id) {
        return players.id(id);
    }

    /**
     * Sets a player online and returns their details, or {@code null} if {@code isOnline} is true
     * and the player was already online.
     */
    public Player setPlayerOnline(Session session, int playerId, boolean isOnline) {
        // A where clause of "" applies no filter, so the isOnline/not-isOnline cases don't need
        // separate builder-reassignment branches.
        var mapper = session.getRecordMappingFactory().getMapper(Player.class);
        // "online" is set above but deliberately not also read back below - see the equivalent
        // note in findPlayerToAttack.
        Optional<Player> player = session.upsert(getPlayerKey(playerId))
                .where(isOnline ? "$.online == false" : "")
                .bin("online").setTo(isOnline)
                .bin("id").get()
                .bin("userName").get()
                .bin("firstName").get()
                .bin("lastName").get()
                .bin("email").get()
                .bin("shieldExpiry").get()
                .bin("beingAttackedBy").get()
                .bin("score").get()
                .execute().getFirst(mapper);
        if (player.isEmpty()) {
            return null;
        }
        player.get().setOnline(isOnline);
        return player.get();
    }

    /** Resets a player to offline, no shield, not being attacked. Sets the score too if >= 0. */
    public void resetPlayerTo(Session session, int playerId, int score) {
        ChainableOperationBuilder op = session.upsert(getPlayerKey(playerId))
                .bin("online").setTo(false)
                .bin("shieldExpiry").setTo(0L)
                .bin("beingAttackedBy").setTo("");
        if (score >= 0) {
            op = op.bin("score").setTo(score);
        }
        op.execute();
    }

    /**
     * Determines if the player can currently be attacked. Not for real game play - the result
     * could be stale by the time it's returned.
     */
    public boolean canAttackPlayerTest(Session session, int playerId) {
        try {
            Optional<RecordResult> result = session.query(getPlayerKey(playerId))
                    .where(getPlayerFilter())
                    .failOnFilteredOut()
                    .execute().getFirst();
            if (result.isEmpty()) {
                System.out.println("*** Key " + playerId + " does not exist!");
                return false;
            }
            result.get().recordOrThrow();
            return true;
        }
        catch (AerospikeException.FilteredException fe) {
            return false;
        }
    }

    public int testEligibility(Session session) {
        int playerId = 1;
        int originalScore = session.query(getPlayerKey(1)).execute().getFirstRecord().getInt("score");
        System.out.printf("%nTesting eligibility for player %d%n", playerId);
        resetPlayerTo(session, playerId, 590);
        System.out.printf("- Checking player can validly be attacked: %s%n",
                canAttackPlayerTest(session, playerId) ? "PASSED" : "FAILED");

        setPlayerOnline(session, playerId, true);
        System.out.printf("- Checking player cannot be attacked when online: %s%n",
                !canAttackPlayerTest(session, playerId) ? "PASSED" : "FAILED");

        resetPlayerTo(session, playerId, 50);
        System.out.printf("- Checking player cannot be attacked with a low score: %s%n",
                !canAttackPlayerTest(session, playerId) ? "PASSED" : "FAILED");

        resetPlayerTo(session, playerId, 500);
        leaderboard.updatePlayerScore(session, playerId, originalScore, 1234);
        return 1234;
    }
}
