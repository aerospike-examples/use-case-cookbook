package com.aerospike.examples.gaming;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Txn;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.examples.Async;
import com.aerospike.examples.Parameter;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.Utils;
import com.aerospike.examples.gaming.model.Player;
import com.aerospike.generator.Generator;
import com.aerospike.mapper.tools.AeroMapper;

public class Leaderboard implements UseCase {
    private static final String SCOREBOARD_BIN = "score";
    private static final MapPolicy MAP_POLICY = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
    
    private static final int SCORES_PER_BUCKET = 25;
    private static final Parameter<Integer> NUM_PLAYERS = new Parameter<>("NUM_PLAYERS", 100_000, "Numberof players in the simulation");
    private static final int MAX_SCORE = 6300; 
    private static final int MAX_BUCKETS = MAX_SCORE/SCORES_PER_BUCKET;
    
    private static final Parameter<Integer> NUM_THREADS = new Parameter<>("NUM_THREADS", 50, "Number of threads to concurrently update the simulation with");
    private static final int PLAYER1_UPDATE_PERIOD = 50; //ms
    private static final int THREAD_UPDATE_PERIOD = 5; //ms
    private static final Parameter<Integer> RUNTIME_SECS = new Parameter<>("RUNTIME_SECS", 20, "Duration of the run in seconds");

    private static final int SCOREBOARD_DISPLAY_PERIOD = 3; //secs
    private String playerNamespace;
    private String playerSet;
    
    private String scoreboardNamespace;
    private String scoreboardSet;

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
                + "their socre every %,d milliseonds, and showing the scoreboard around their score every %d seconds. "
                + "Additionally there are %,d background threads which are randomly updating other scores every %,dms "
                + "per thread.",
                PLAYER1_UPDATE_PERIOD, SCOREBOARD_DISPLAY_PERIOD, NUM_THREADS.get(), THREAD_UPDATE_PERIOD);
    }
    
    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/leaderboard.md";
    }
    
    @Override
    public String[] getTags() {
        return new String[] {"Transactions", "Map Operations", "Expressions" };
    }

    @Override
    public Parameter<?>[] getParams() {
        return new Parameter<?>[] {NUM_PLAYERS, NUM_THREADS, RUNTIME_SECS};
    }
    
    public void setDefaultValues(AeroMapper mapper) {
        this.playerNamespace = mapper.getNamespace(Player.class);
        this.playerSet = mapper.getSet(Player.class);
        this.scoreboardNamespace = this.playerNamespace;
        this.scoreboardSet = "scoreboard";
    }

    @Override
    public void setup(IAerospikeClient client, AeroMapper mapper) throws Exception {
        setDefaultValues(mapper);
        client.truncate(null, playerNamespace, playerSet, null); 
        client.truncate(null, scoreboardNamespace, scoreboardSet, null); 
        
        System.out.printf("Generating %,d Players\n", NUM_PLAYERS.get());
        new Generator(Player.class)
            .generate(1, NUM_PLAYERS.get(), Player.class, player -> {
              mapper.save(player);
              updatePlayerScore(client, player.getId(), -1, player.getScore(), null);
            })
            .monitor();
    }

    @Override
    public void run(IAerospikeClient client, AeroMapper mapper) throws Exception {
        setDefaultValues(mapper);
        Record res = client.get(null, new Key(playerNamespace, playerSet, 1));
        int score = res.getInt("score");
        final int playerId = res.getInt("id");
        showPlayersAroundPlayer(client, playerId, score);
        
        System.out.printf("%nStarting gaming for %ds...%n", RUNTIME_SECS.get());
        final AtomicInteger scoreVal = new AtomicInteger(score);
        
        Async.runFor(Duration.ofSeconds(RUNTIME_SECS.get()), (async) -> {
            // Show the scoreboard every 2 seconds.
            async.periodic(Duration.ofSeconds(SCOREBOARD_DISPLAY_PERIOD), () -> {
                // Ensure that player 1's score does not change while reading the scoreboard data! (Could be
                // done using transactions, but not all users will have transaction support, and it's an
                // unrealistic use case anyway.)
                synchronized (async) {
                    showPlayersAroundPlayer(client, playerId, scoreVal.get());
                }
            });
            
            // Change the main player score every 50ms, having it slowly creep up randomly
            async.periodic(Duration.ofMillis(PLAYER1_UPDATE_PERIOD), () -> {
                synchronized (async) {
                    int currentScore = scoreVal.get();
                    int newScore = changeScore(currentScore, 1);
                    updatePlayerScore(client, playerId, currentScore, newScore, null);
                    scoreVal.set(newScore);
                }
            });
            
            // To run the updating of a random player's score as fast as possible use the following code
//            async.continuous(NUM_THREADS, () -> {
//                // Ensure we do not update player 1's score in this thread
//                int playerIdToChange = async.rand().nextInt(NUM_PLAYERS.get()-1)+2;
//                Record record = client.get(null, new Key(playerNamespace, playerSet, playerIdToChange));
//                if (record != null) {
//                    int currentScore = record.getInt("score");
//                    updatePlayerScore(client, playerIdToChange, currentScore, changeScore(currentScore));
//                }
//                
//            });

            // Update a random player's score
            async.periodic(Duration.ofMillis(THREAD_UPDATE_PERIOD), NUM_THREADS.get(), () -> {
                // Ensure we do not update player 1's score in this thread
                int playerIdToChange = async.rand().nextInt(NUM_PLAYERS.get()-1)+2;
                Record record = client.get(null, new Key(playerNamespace, playerSet, playerIdToChange));
                if (record != null) {
                    int currentScore = record.getInt("score");
                    updatePlayerScore(client, playerIdToChange, currentScore, changeScore(currentScore), null);
                }
                
            });
        });
        
        System.out.printf("\n************************");
        System.out.printf("\n*** Final scoreboard ***");
        System.out.printf("\n************************\n");
        showPlayersAroundPlayer(client, playerId, scoreVal.get());
    }
    
    
    public String getScoreboardNamespace() {
        return scoreboardNamespace;
    }
    public String getScoreboardSet() {
        return scoreboardSet;
    }
    
    private int changeScore(int currentScore) {
        return changeScore(currentScore, 0);
    }
    private int changeScore(int currentScore, int offset) {
        int scoreChange = ThreadLocalRandom.current().nextInt(49) - 24 + offset;
        return Math.max(0, Math.min(6200, currentScore) + scoreChange);
    }
    
    private void populateFullPlayerDetails(IAerospikeClient client, List<Player> players) {
        Key[] keys = players.stream()
                .map(player -> new Key(playerNamespace, playerSet, player.getId()))
                .toArray(Key[]::new);
        Record[] records = client.get(null, keys);
        for (int i = 0; i < records.length; i++) {
            Record thisRecord = records[i];
            if (thisRecord != null) {
                Player thisPlayer = players.get(i);
                thisPlayer.setUserName(thisRecord.getString("userName"));
                thisPlayer.setFirstName(thisRecord.getString("firstName"));
                thisPlayer.setLastName(thisRecord.getString("lastName"));
                thisPlayer.setEmail(thisRecord.getString("email"));
                thisPlayer.setShieldExpiry(thisRecord.getLong("shieldExpiry"));
                thisPlayer.setOnline(thisRecord.getBoolean("online"));
                thisPlayer.setBeingAttackedBy(thisRecord.getString("beingAttackedBy"));
            }
        }
    }
    
    public void showPlayersAroundPlayer(IAerospikeClient client, int playerId, int score) {
        List<Player> players = getScoresAroundPlayer(client, playerId, score, 6);
        populateFullPlayerDetails(client, players);
        System.out.printf("\nCurrent scoreboard around player %d\n", playerId);
        System.out.println("Score |                     User Name                      |  Id ");
        System.out.println("-----------------------------------------------------------------");
        for (Player thisPlayer : players) {
            if (thisPlayer.getId() == playerId) {
                System.out.printf("\033[1m");
            }
            System.out.printf("%5s | %50s | %d\n", 
                    ""+thisPlayer.getScore(), thisPlayer.getUserName(), thisPlayer.getId());
            if (thisPlayer.getId() == playerId) {
                System.out.printf("\033[0m");
            }
        }
    }
    private int determineBucketForScore(int score) {
        return score / SCORES_PER_BUCKET;
    }
    
    private Key getScoreboardKey(int score) {
        return new Key(scoreboardNamespace, scoreboardSet, determineBucketForScore(score));
    }
    
    /**
     * Combine the player id and the score into a composite string. The string will always
     * be the same length and higher scores will return higher strings
     * @param playerId - The id of the player
     * @param score - the score for the player
     * @return a String combining the score and the player id.
     */
    private String getMapKey(int playerId, int score) {
        return String.format("%05d-%09d", score, playerId);
    }
    
    /**
     * Turn a map key comprised of a composite string (score-playerId) into 
     * a player with just these 2 fields populated
     * @param mapKey
     * @return
     */
    private Player mapKeyToScoreDetails(String mapKey) {
        String[] parts = mapKey.split("-");
        Player result = new Player();
        result.setScore(Integer.parseInt(parts[0]));
        result.setId(Integer.parseInt(parts[1]));
        return result;
    }

    /**
     * Set the score of the player to the new score. This will both update the score on the Player set
     * and update the position in the leader board.
     * @param client
     * @param playerId
     * @param oldScore
     * @param newScore
     * @param existingTxn - The transaction id of any transaction which wrappers this method, or null
     */
    public void updatePlayerScore(IAerospikeClient client, int playerId, int oldScore, int newScore, Txn existingTxn) {
        Utils.doInTransaction(client, txn -> {
            WritePolicy writePolicy = client.copyWritePolicyDefault();
            writePolicy.txn = txn;
            
            Key newBucketKey = getScoreboardKey(newScore);
            if (oldScore < 0) {
                // This is just an insert
                String mapKey = getMapKey(playerId, newScore);
                
                client.operate(writePolicy, newBucketKey, 
                        MapOperation.put(MAP_POLICY, SCOREBOARD_BIN, Value.get(mapKey), Value.get(playerId)));
            }
            else {
                String newMapKey = getMapKey(playerId, newScore);
                String oldMapKey = getMapKey(playerId, oldScore);

                if (determineBucketForScore(newScore) == determineBucketForScore(oldScore)) {
                    // These are in the same record, they can be done atomically
                    client.operate(writePolicy, newBucketKey, 
                            MapOperation.removeByKey(SCOREBOARD_BIN, Value.get(oldMapKey), MapReturnType.NONE),
                            MapOperation.put(MAP_POLICY, SCOREBOARD_BIN, Value.get(newMapKey), Value.get(playerId)));
                }
                else {
                    // The keys are in two separate buckets. Must be done in two separate calls
                    Key oldBucketKey = getScoreboardKey(oldScore);
                    client.operate(writePolicy, oldBucketKey, 
                            MapOperation.removeByKey(SCOREBOARD_BIN, Value.get(oldMapKey), MapReturnType.NONE));
                    client.operate(writePolicy, newBucketKey,
                            MapOperation.put(MAP_POLICY, SCOREBOARD_BIN, Value.get(newMapKey), Value.get(playerId)));
                }
            }
            if (oldScore >= 0) {
                // Update the record if there was an old score.
                client.put(writePolicy, new Key(playerNamespace, playerSet, playerId), new Bin("score", newScore));
            }
        });
    }

    public List<Player> getScoresAroundPlayer(IAerospikeClient client, Player player, int numPlayersEitherSide) {
        return getScoresAroundPlayer(client, player.getId(), player.getScore(), numPlayersEitherSide);
    }

    /**
     * Get the scores on either side of the player. This will query the rank of scores staring at the players rank
     * in the desired map and determine the {@code numPlayerEitherSide} of this score. Note that this might require
     * some scores in the next record, either above or below. For example, if the score is 349, the next record bound
     * starts at 350, so we might need to query scores in that one too.
     * <p/>
     * Note that this returns a list of players with just their {@code id} and {@code score} filled in. The other details are 
     * not populated.
     * @param client - The Aerospike client instance to use
     * @param playerId - the playerId to get the scores of
     * @param score - the score of that player.
     * @param numPlayersEitherSide - how many players to return either side.
     * @return
     */
    public List<Player> getScoresAroundPlayer(IAerospikeClient client, int playerId, int score, int numPlayersEitherSide) {
        String mapKey = getMapKey(playerId, score);

        // We need to use an expression here, and a rather nasty one as we want to make sure the index
        // does not go negative. For example, if this player's index is 5 and we ask for 10 either side
        // we cannot just return 5-10 = -5 as this will cause wrap around. Our expression is:
        Exp lowerPlayers = Exp.let(
          Exp.def("index",
            MapExp.getByKey(
              MapReturnType.INDEX,
              Exp.Type.INT,
              Exp.val(mapKey),
              Exp.mapBin(SCOREBOARD_BIN)
            )
          ),
          Exp.def("startIndex",
            Exp.cond(
              Exp.ge(
                Exp.var("index"), 
                Exp.val(numPlayersEitherSide)
              ), Exp.sub(Exp.var("index"), Exp.val(numPlayersEitherSide)),
              Exp.val(0)
            )
          ),
          Exp.def("count",
            Exp.cond(
              Exp.ge(
                Exp.var("index"), 
                Exp.val(numPlayersEitherSide)
              ), Exp.val(numPlayersEitherSide),
              Exp.var("index")
            )
          ),
          MapExp.getByIndexRange(
            MapReturnType.KEY,
            Exp.var("startIndex"),
            Exp.var("count"),
            Exp.mapBin(SCOREBOARD_BIN)
          )
        );
        
        Exp higherPlayers = Exp.let(
          Exp.def("index",
            MapExp.getByKey(
              MapReturnType.INDEX,
              Exp.Type.INT,
              Exp.val(mapKey),
              Exp.mapBin(SCOREBOARD_BIN)
            )
          ),
          MapExp.getByIndexRange(
            MapReturnType.KEY,
            Exp.var("index"),
            Exp.val(numPlayersEitherSide+1),
            Exp.mapBin(SCOREBOARD_BIN)
          )
        );

        int playerBucket = determineBucketForScore(score);
        Record result = client.operate(null, getScoreboardKey(score), 
                ExpOperation.read("lowerPlayers", Exp.build(lowerPlayers), ExpReadFlags.DEFAULT),
                ExpOperation.read("higherPlayers", Exp.build(higherPlayers), ExpReadFlags.DEFAULT)
            );
        List<String> lowerPlayersList = (List<String>) result.getList("lowerPlayers");
        List<String> higherPlayersList = (List<String>) result.getList("higherPlayers");

        // Check to see if blocks overflowed
        addOverflowLowerPlayersIfNeeded(client, lowerPlayersList, playerBucket, numPlayersEitherSide);
        addOverflowHigherPlayersIfNeeded(client, higherPlayersList, playerBucket, numPlayersEitherSide);
        
        List<Player> allPlayers = new ArrayList<>(lowerPlayersList.size() + higherPlayersList.size());
        lowerPlayersList.forEach(item -> allPlayers.add(mapKeyToScoreDetails(item)));
        higherPlayersList.forEach(item -> allPlayers.add(mapKeyToScoreDetails(item)));

        return allPlayers;
    }
    
    private void addOverflowLowerPlayersIfNeeded(IAerospikeClient client, List<String> currentPlayers, int playerBucket, int numPlayersEitherSide) {
        int currentBucket = playerBucket;
        while (currentBucket-- >= 0 && currentPlayers.size() < numPlayersEitherSide) {
            Record result = client.operate(null, new Key(scoreboardNamespace, scoreboardSet, currentBucket),
                    MapOperation.getByIndexRange(SCOREBOARD_BIN, currentPlayers.size()-numPlayersEitherSide, MapReturnType.KEY));
            if (result != null) {
                currentPlayers.addAll(0, (List<String>) result.getList(SCOREBOARD_BIN));
            }
        }
    }

    private void addOverflowHigherPlayersIfNeeded(IAerospikeClient client, List<String> currentPlayers, int playerBucket, int numPlayersEitherSide) {
        int currentBucket = playerBucket;
        while (currentBucket++ <= MAX_BUCKETS && currentPlayers.size() < numPlayersEitherSide+1) {
            Record result = client.operate(null, new Key(scoreboardNamespace, scoreboardSet, currentBucket),
                    MapOperation.getByIndexRange(SCOREBOARD_BIN, 0, numPlayersEitherSide+1 - currentPlayers.size(), MapReturnType.KEY));
            if (result != null) {
                currentPlayers.addAll((List<String>) result.getList(SCOREBOARD_BIN));
            }
        }
    }

}
