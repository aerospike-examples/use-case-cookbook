package com.aerospike.examples.gaming;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.TypedDataSet;
import com.aerospike.client.sdk.TypedKeyList;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.examples.Async;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.gaming.model.Player;

/**
 * SDK port of the legacy {@code Leaderboard} (see ../../java). A scoreboard is stored as a
 * key-ordered map per score "bucket" ({@code score / SCORES_PER_BUCKET}), where each map key is a
 * zero-padded {@code "score-playerId"} composite string (so map key order == score order) and each
 * map value is the player id. Reading the players around a given score/player uses an expression
 * to find that player's index within its bucket's map and then a clamped index range either side
 * of it, falling back to neighboring buckets if the range overflows the current one.
 */
public class Leaderboard implements UseCase {

    private static final String SCOREBOARD_BIN = "score";

    private static final int SCORES_PER_BUCKET = 25;
    private static final int NUM_PLAYERS = 100_000;
    private static final int MAX_SCORE = 6300;
    private static final int MAX_BUCKETS = MAX_SCORE / SCORES_PER_BUCKET;

    private static final int NUM_THREADS = 50;
    private static final int PLAYER1_UPDATE_PERIOD = 50; // ms
    private static final int THREAD_UPDATE_PERIOD = 5; // ms
    private static final int RUNTIME_SECS = 20;

    private static final int SCOREBOARD_DISPLAY_PERIOD = 3; // secs

    private static final String[] FIRST_NAMES = {"Estefana", "Arnoldo", "Jacquelyne", "Harris", "Ardelia"};
    private static final String[] LAST_NAMES = {"Ruecker", "MacGyver", "Willms", "Jones", "Renner"};

    @Override
    public String getName() {
        return "Gaming Leaderboard";
    }

    @Override
    public String getDescription() {
        return String.format(
                "Demonstrate an approach to create a leaderboard in Aerospike. When a player achieves a score in "
                + "a game the leaderboard should be updated to reflect their score. The leaderboard around a "
                + "particular score can be shown. When running, one player (id = 1) will be focused on, changing "
                + "their score every %,d milliseconds, and showing the scoreboard around their score every %d seconds. "
                + "Additionally there are %,d background threads which are randomly updating other scores every %,dms "
                + "per thread.",
                PLAYER1_UPDATE_PERIOD, SCOREBOARD_DISPLAY_PERIOD, NUM_THREADS, THREAD_UPDATE_PERIOD);
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/leaderboard.md";
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
            updatePlayerScore(session, player.getId(), -1, player.getScore());
        }
    }

    @Override
    public void run(Session session) throws Exception {
        Record record = session.query(players.id(1)).execute().getFirstRecord();
        int score = record.getInt("score");
        final int playerId = record.getInt("id");
        showPlayersAroundPlayer(session, playerId, score);

        System.out.printf("%nStarting gaming for %ds...%n", RUNTIME_SECS);
        final AtomicInteger scoreVal = new AtomicInteger(score);

        Async.runFor(Duration.ofSeconds(RUNTIME_SECS), (async) -> {
            async.periodic(Duration.ofSeconds(SCOREBOARD_DISPLAY_PERIOD), () -> {
                synchronized (async) {
                    showPlayersAroundPlayer(session, playerId, scoreVal.get());
                }
            });

            async.periodic(Duration.ofMillis(PLAYER1_UPDATE_PERIOD), () -> {
                synchronized (async) {
                    int currentScore = scoreVal.get();
                    int newScore = changeScore(currentScore, 1);
                    updatePlayerScore(session, playerId, currentScore, newScore);
                    scoreVal.set(newScore);
                }
            });

            async.periodic(Duration.ofMillis(THREAD_UPDATE_PERIOD), NUM_THREADS, () -> {
                int playerIdToChange = async.rand().nextInt(NUM_PLAYERS - 1) + 2;
                Record playerRecord = session.query(players.id(playerIdToChange)).execute().getFirstRecord();
                if (playerRecord != null) {
                    int currentScore = playerRecord.getInt("score");
                    updatePlayerScore(session, playerIdToChange, currentScore, changeScore(currentScore));
                }
            });
        });

        System.out.printf("%n************************");
        System.out.printf("%n*** Final scoreboard ***");
        System.out.printf("%n************************%n");
        showPlayersAroundPlayer(session, playerId, scoreVal.get());
    }

    private int changeScore(int currentScore) {
        return changeScore(currentScore, 0);
    }

    private int changeScore(int currentScore, int offset) {
        int scoreChange = ThreadLocalRandom.current().nextInt(49) - 24 + offset;
        return Math.max(0, Math.min(6200, currentScore) + scoreChange);
    }

    private List<Player> populateFullPlayerDetails(Session session, List<Player> partialPlayers) {
        TypedKeyList<Player> keys = new TypedKeyList<>();
        partialPlayers.forEach(player -> keys.add(players.id(player.getId())));
        return session.query(keys).execute().toObjectList();
    }

    public void showPlayersAroundPlayer(Session session, int playerId, int score) {
        List<Player> playerList = populateFullPlayerDetails(session, getScoresAroundPlayer(session, playerId, score, 6));
        System.out.printf("%nCurrent scoreboard around player %d%n", playerId);
        System.out.println("Score |                     User Name                      |  Id ");
        System.out.println("-----------------------------------------------------------------");
        for (Player thisPlayer : playerList) {
            if (thisPlayer.getId() == playerId) {
                System.out.printf("\033[1m");
            }
            System.out.printf("%5s | %50s | %d%n",
                    "" + thisPlayer.getScore(), thisPlayer.getUserName(), thisPlayer.getId());
            if (thisPlayer.getId() == playerId) {
                System.out.printf("\033[0m");
            }
        }
    }

    private int determineBucketForScore(int score) {
        return score / SCORES_PER_BUCKET;
    }

    private Key getScoreboardKey(int score) {
        return scoreboard.id(determineBucketForScore(score));
    }

    private String getMapKey(int playerId, int score) {
        return String.format("%05d-%09d", score, playerId);
    }

    private Player mapKeyToScoreDetails(String mapKey) {
        String[] parts = mapKey.split("-");
        Player result = new Player();
        result.setScore(Integer.parseInt(parts[0]));
        result.setId(Integer.parseInt(parts[1]));
        return result;
    }

    /**
     * Sets the score of the player, both on the player record and the scoreboard's map.
     */
    public void updatePlayerScore(Session session, int playerId, int oldScore, int newScore) {
        session.doInTransaction(tx -> {
            Key newBucketKey = getScoreboardKey(newScore);
            if (oldScore < 0) {
                String mapKey = getMapKey(playerId, newScore);
                tx.upsert(newBucketKey)
                        .bin(SCOREBOARD_BIN).onMapKey(mapKey, MapOrder.KEY_ORDERED).upsert(playerId)
                        .execute();
            }
            else {
                String newMapKey = getMapKey(playerId, newScore);
                String oldMapKey = getMapKey(playerId, oldScore);

                if (determineBucketForScore(newScore) == determineBucketForScore(oldScore)) {
                    tx.upsert(newBucketKey)
                            .bin(SCOREBOARD_BIN).onMapKey(oldMapKey).remove()
                            .bin(SCOREBOARD_BIN).onMapKey(newMapKey, MapOrder.KEY_ORDERED).upsert(playerId)
                            .execute();
                }
                else {
                    Key oldBucketKey = getScoreboardKey(oldScore);
                    tx.upsert(oldBucketKey).bin(SCOREBOARD_BIN).onMapKey(oldMapKey).remove().execute();
                    tx.upsert(newBucketKey)
                            .bin(SCOREBOARD_BIN).onMapKey(newMapKey, MapOrder.KEY_ORDERED).upsert(playerId)
                            .execute();
                }
            }
            if (oldScore >= 0) {
                tx.upsert(players.id(playerId)).bin("score").setTo(newScore).execute();
            }
        });
    }

    /**
     * Gets the scores on either side of a player's score. Reads a clamped index range either side
     * of the player's map key directly, pulling in extra entries from neighboring buckets if the
     * range overflows the current bucket.
     * <p/>
     * Written as a single AEL relative-range map selector ({@code {-N:N~key}} - see the canonical
     * reference at ../../AEL_CANONICAL_REFERENCE.md, section 5) rather than the original nested
     * {@code Exp.let}/{@code Exp.def}/{@code Exp.cond} + two separate {@code MapExp} reads. This
     * replaces the whole index-lookup-then-clamped-range composition with one read, and the server
     * clamps automatically at the map's boundaries (no manual {@code startIndex}/{@code count}
     * clamping needed for the same-bucket portion). Verified against a live cluster with a full
     * side-by-side diff against the original Exp composition across many buckets - byte-identical
     * results. The earlier conclusion that this needed unsupported nested-expression selectors was
     * based on a non-canonical grammar reference; the canonical AEL reference's relative-range
     * selector form solves it directly.
     */
    public List<Player> getScoresAroundPlayer(Session session, int playerId, int score, int numPlayersEitherSide) {
        String mapKey = getMapKey(playerId, score);
        String escapedKey = mapKey.replace("'", "\\'");
        String ael = String.format("$.%s.{-%d:%d~'%s'}.getKeys()",
                SCOREBOARD_BIN, numPlayersEitherSide, numPlayersEitherSide, escapedKey);

        int playerBucket = determineBucketForScore(score);
        Record result = session.query(getScoreboardKey(score))
                .bin("combined").selectFrom(ael)
                .execute().getFirstRecord();

        @SuppressWarnings("unchecked")
        List<String> combined = new ArrayList<>((List<String>) result.getList("combined"));
        int keyPos = combined.indexOf(mapKey);
        List<String> lowerPlayersList = new ArrayList<>(combined.subList(0, keyPos));
        List<String> higherPlayersList = new ArrayList<>(combined.subList(keyPos, combined.size()));

        addOverflowLowerPlayersIfNeeded(session, lowerPlayersList, playerBucket, numPlayersEitherSide);
        addOverflowHigherPlayersIfNeeded(session, higherPlayersList, playerBucket, numPlayersEitherSide);

        List<Player> allPlayers = new ArrayList<>(lowerPlayersList.size() + higherPlayersList.size());
        lowerPlayersList.forEach(item -> allPlayers.add(mapKeyToScoreDetails(item)));
        higherPlayersList.forEach(item -> allPlayers.add(mapKeyToScoreDetails(item)));

        return allPlayers;
    }

    private void addOverflowLowerPlayersIfNeeded(Session session, List<String> currentPlayers, int playerBucket, int numPlayersEitherSide) {
        int currentBucket = playerBucket;
        while (currentBucket-- >= 0 && currentPlayers.size() < numPlayersEitherSide) {
            Record record = session.query(scoreboard.id(currentBucket))
                    .bin(SCOREBOARD_BIN).onMapIndexRange(currentPlayers.size() - numPlayersEitherSide).getKeys()
                    .execute().getFirstRecord();
            if (record != null) {
                @SuppressWarnings("unchecked")
                List<String> extra = (List<String>) record.getList(SCOREBOARD_BIN);
                currentPlayers.addAll(0, extra);
            }
        }
    }

    private void addOverflowHigherPlayersIfNeeded(Session session, List<String> currentPlayers, int playerBucket, int numPlayersEitherSide) {
        int currentBucket = playerBucket;
        while (currentBucket++ <= MAX_BUCKETS && currentPlayers.size() < numPlayersEitherSide + 1) {
            Record record = session.query(scoreboard.id(currentBucket))
                    .bin(SCOREBOARD_BIN).onMapIndexRange(0, numPlayersEitherSide + 1 - currentPlayers.size()).getKeys()
                    .execute().getFirstRecord();
            if (record != null) {
                @SuppressWarnings("unchecked")
                List<String> extra = (List<String>) record.getList(SCOREBOARD_BIN);
                currentPlayers.addAll(extra);
            }
        }
    }
}
