package com.aerospike.examples.gaming;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.examples.Async;
import com.aerospike.examples.Parameter;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.Utils;
import com.aerospike.examples.gaming.model.Player;
import com.aerospike.generator.Generator;
import com.aerospike.mapper.tools.AeroMapper;

public class PlayerMatching implements UseCase {
    private static final Parameter<Integer> NUM_PLAYERS = new Parameter<>("NUM_PLAYERS", 10_000, "The number of players in the simulation");
    private static final Parameter<Integer> RUNTIME_SECS = new Parameter<>("RUNTIME_SECS", 10, "The amount of time to run the simulation in seconds");
    private static long shieldDurationMs = TimeUnit.SECONDS.toMillis(5);
    private String playerNamespace;
    private String playerSet;
    
    private final Leaderboard leaderboard;
    
    public PlayerMatching() {
        super();
        this.leaderboard = new Leaderboard();
    }

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
                + "to attack they need to match against another player. The player cannot be online in this example "
                + "example, as they may be playing the game. In order to match, we want another player who:\n"
                + "1. Is not online\n"
                + "2. Who does not have a shield\n"
                + "3. Who is not currently being attacked\n"
                + "4. Whose score is >= 400\n"
                + "5. Whose score is similar to this player\n "
                + "A shield is given to a player when they have been attacked and defeated. These would normally be a decent length "
                + "to stop them being attacked too often, but in this example will only be for 5s. Attackers defeat "
                + "their opponents 80% of the time. If a player with a shiled attacks "
                + "during a shield, the shield is removed.";
    }
    
    @Override
    public String[] getTags() {
        return new String[] {"Expressions", "Operations"};
    }

    @Override
    public Parameter<?>[] getParams() {
        return new Parameter<?>[] {NUM_PLAYERS, RUNTIME_SECS};
    }
    
    private void setup(AeroMapper mapper) {
        leaderboard.setDefaultValues(mapper);
        playerNamespace = mapper.getNamespace(Player.class);
        playerSet = mapper.getSet(Player.class);
    }
    @Override
    public void setup(IAerospikeClient client, AeroMapper mapper) throws Exception {
        setup(mapper);
        client.truncate(null, playerNamespace, playerSet, null);
        client.truncate(null, this.leaderboard.getScoreboardNamespace(), this.leaderboard.getScoreboardSet(), null);
        
        System.out.printf("Generating %,d Players\n", NUM_PLAYERS.get());
        new Generator(Player.class)
            .generate(1, NUM_PLAYERS.get(), Player.class, player -> {
              mapper.save(player);
              leaderboard.updatePlayerScore(client, player.getId(), -1, player.getScore(), null);
        })
        .monitor();
    }

    @Override
    public void run(IAerospikeClient client, AeroMapper mapper) throws Exception {
        setup(mapper);
        testEligibility(client);
        
        System.out.printf("\nLet's play some games!\n");
        Player player1 = setPlayerOnline(client, 1, true);
        System.out.printf("Leader board before the games start...\n");
        leaderboard.showPlayersAroundPlayer(client, player1.getId(), player1.getScore());
        
        AtomicInteger counter = new AtomicInteger();
        Async.runFor(Duration.ofSeconds(RUNTIME_SECS.get()), (async) -> {
            async.periodic(Duration.ofMillis(100), () -> {
                findPlayerToAttack(client, player1).ifPresent(defender -> {
                    playGame(client, player1, defender, true);
                    counter.incrementAndGet();
                });
            });
            
            async.periodic(Duration.ofMillis(5), 20, () -> {
                // Pick any player besides player one.
                int playerId = async.rand().nextInt(NUM_PLAYERS.get()-1) + 2;
                Player player = setPlayerOnline(client, playerId, true);
                if (player != null) {
                    // This player is valid, and not online
                    try {
                        findPlayerToAttack(client, player).ifPresent(defender -> {
                            playGame(client, player, defender, false);
                            counter.incrementAndGet();
                        });
                    }
                    finally {
                        setPlayerOnline(client, playerId, false);
                    }
                }
            });
        });
        System.out.printf("Leader board after the games end...\n");
        leaderboard.showPlayersAroundPlayer(client, player1.getId(), player1.getScore());
        
        System.out.printf("Total battles played (all threads): %,d\n", counter.get());
        setPlayerOnline(client, player1.getId(), false);
    }
    
    /**
     * Find a player to attack in the list of possibilities. The player must meet the filter criteria and one
     * will be selected, tested for eligibility and if passed set to be being under attack atomically. Also the 
     * details of th player are returned.
     * @param client - The Aerospike client to use
     * @param attackerId - The id of the attacker
     * @param possibilities - A list of possibilities who could be attacked.
     * @return the player details on someone to attack. The player will already have been locked for attacking.
     */
    public Optional<Player> findPlayerToAttack(IAerospikeClient client, int attackerId, Key[] possibilities) {
        // We have a list of players, but some might invalid
        BatchPolicy batchPolicy = client.copyBatchPolicyDefault();
        batchPolicy.filterExp = getPlayerFilter();

        Record[] records = client.get(batchPolicy, possibilities);
        List<Record> validRecords = new ArrayList<>();
        for (Record record : records) {
            if (record != null) {
                validRecords.add(record);
            }
        }
        // Try each element in the list randomly until either the list is empty or we have a match.
        WritePolicy writePolicy = client.copyWritePolicyDefault();
        writePolicy.filterExp = batchPolicy.filterExp;
        while (validRecords.size() > 0) {
            int recordNum = ThreadLocalRandom.current().nextInt(validRecords.size());
            int id = validRecords.get(recordNum).getInt("id");
            Record result = client.operate(writePolicy, getPlayerKey(id),
                    Operation.put(new Bin("beingAttackedBy", "Player " + id)),
                    Operation.get("id"),
                    Operation.get("userName"),
                    Operation.get("firstName"),
                    Operation.get("lastName"),
                    Operation.get("email"),
                    Operation.get("shieldExpiry"),
                    Operation.get("online"),
                    Operation.get("beingAttackedBy"),
                    Operation.get("score"));

            if (result == null) {
                // This person is no longer valid
                validRecords.remove(recordNum);
            }
            else {
                return Optional.of(recordToPlayer(result));
            }
        }
        return Optional.empty();
    }
    
    /**
     * Given a player, find a player close to their own strength who is available to attack.
     * @param client - The Aerospike client to use
     * @param attacker - The attacker who we want to find a match for. 
     * @return the player details on someone to attack. The player will already have been locked for attacking.
     */
    public Optional<Player> findPlayerToAttack(IAerospikeClient client, Player attacker) {
        List<Player> similarScores = leaderboard.getScoresAroundPlayer(client, attacker.getId(), attacker.getScore(), 20);
        // Turn the list of players into a set of keys, exluding this attacker's
        Key[] keys = similarScores.stream()
                .filter(player -> player.getId() != attacker.getId())
                .map(player -> new Key(playerNamespace, playerSet, player.getId()))
                .toArray(Key[]::new);
        return findPlayerToAttack(client, attacker.getId(), keys);
    }
    
    /**
     * Calculates the new Elo rating for a player after a game.
     *
     * @param playerRating   The current rating of the player (e.g., 1600).
     * @param opponentRating The rating of the opponent (e.g., 1800).
     * @param score          The actual score of the game for the player:
     *                       1 = win, 0 = draw, 0 = loss.
     * @param kFactor        The K-factor used to determine rating volatility (e.g., 20).
     * @return The new rating of the player after the game, rounded to the nearest integer.
     */
    public static int calculateNewEloRating(int playerRating, int opponentRating, int score, int kFactor) {
        // Convert score to double for calculation
        double scoreDouble = score;

        // Calculate expected score using the Elo formula
        double expectedScore = 1.0 / (1.0 + Math.pow(10.0, (opponentRating - playerRating) / 400.0));

        // Update rating and round to nearest integer
        double newRating = playerRating + kFactor * (scoreDouble - expectedScore);
        return (int) Math.round(newRating);
    }

    /**
     * Play a game between an attacker and a defender. When a winner is known, scores will be adjusted,
     * the leader-board updated and the "under attack" flag is remmoved from the defender. 
     * @param client - The Aerospike client to use
     * @param attacker - The attacker in the game
     * @param defender - The defender in the game
     * @param showBattle - If true, output is logged as the game is won or lost as well as score adjustments
     */
    public void playGame(IAerospikeClient client, Player attacker, Player defender, boolean showBattle) {
        if (attacker == null || defender == null) {
            return;
        }
        
        if (showBattle) {
            System.out.printf("%s is attacking %s... ", attacker.getUserName(), defender.getUserName());
        }
        // Simulate some play time
        try {
            Thread.sleep(5);
        } catch (InterruptedException ignored) {
        }
        int originalAttackerScore = attacker.getScore();
        int originalDefenderScore = defender.getScore();
        
        int probabilityOfWinning = 60 + (attacker.getScore() - defender.getScore())/5;
        if (ThreadLocalRandom.current().nextInt(101) >= probabilityOfWinning) {
            // Attacker won the game
            attacker.setScore(calculateNewEloRating(attacker.getScore(), defender.getScore(), 1, 20));
            defender.setScore(calculateNewEloRating(defender.getScore(), attacker.getScore(), 0, 20));
            defender.setShieldExpiry(new Date().getTime() + shieldDurationMs);
            if (showBattle) {
                System.out.printf("VICTORIOUS! (attacker: %,d -> %,d, defender: %,d->%,d)\n",
                        originalAttackerScore, attacker.getScore(),
                        originalDefenderScore, defender.getScore());
            }
        }
        else {
            // Defender won
            attacker.setScore(calculateNewEloRating(attacker.getScore(), defender.getScore(), 0, 20));
            defender.setScore(calculateNewEloRating(defender.getScore(), attacker.getScore(), 1, 20));
            if (showBattle) {
                System.out.printf("REPELLED! (attacker: %,d -> %,d, defender: %,d->%,d)\n",
                        originalAttackerScore, attacker.getScore(),
                        originalDefenderScore, defender.getScore());
            }
        }
        attacker.setShieldExpiry(0);
        defender.setBeingAttackedBy(null);

        Utils.doInTransaction(client, txn -> {
            WritePolicy wp = client.copyWritePolicyDefault();
            wp.txn = txn;
            
            leaderboard.updatePlayerScore(client, attacker.getId(), originalAttackerScore, attacker.getScore(), txn);
            leaderboard.updatePlayerScore(client, defender.getId(), originalDefenderScore, defender.getScore(), txn);

            client.put(wp, getPlayerKey(attacker.getId()), 
                    new Bin("score", attacker.getScore()),
                    new Bin("shieldExpiry", attacker.getShieldExpiry()));
            
            client.put(wp, getPlayerKey(defender.getId()), 
                    new Bin("score", defender.getScore()),
                    new Bin("shieldExpiry", defender.getShieldExpiry()),
                    new Bin("beingAttackedBy", defender.getBeingAttackedBy()));
        });
    }
    
    /**
     * This method generates an expression to determine player eligibility. Eligibility tested is:
     * <ol>
     * <li>Is not online</li>
     * <li>Does not have an active shield</li>
     * <li>Is not currently bein attacked</li>
     * <li>Score > 400</li>
     * @return an Expression which can be used as a filter to match these criteria
     */
    private Expression getPlayerFilter() {
        Exp exp = Exp.and(
                Exp.not(Exp.boolBin("online")),
                Exp.lt(Exp.intBin("shieldExpiry"), Exp.val(new Date().getTime())),
                Exp.or(Exp.eq(Exp.val(""), Exp.stringBin("beingAttackedBy")), Exp.not(Exp.binExists("beingAttackedBy"))),
                Exp.gt(Exp.intBin("score"), Exp.val(400))
            );
        return Exp.build(exp);
    }

    public Key getPlayerKey(int id) {
        return new Key(playerNamespace, playerSet, id);
    }
    
    /**
     * Turn an Aerospike record into a Player
     * @param record - The record read from the database 
     * @return a Player 
     */
    public Player recordToPlayer(Record record) {
        if (record != null) {
            Player result = new Player();
            result.setBeingAttackedBy(record.getString("beingAttackedBy"));
            result.setEmail(record.getString("email"));
            result.setFirstName(record.getString("firstName"));
            result.setId(record.getInt("id"));
            result.setLastName(record.getString("lastName"));
            result.setOnline(record.getBoolean("online"));
            result.setScore(record.getInt("score"));
            result.setShieldExpiry(record.getLong("shieldExpiry"));
            result.setUserName(record.getString("userName"));
            return result;
        }
        return null;
    }
    
    /** 
     * Set a player to be online and get the player details. If the player is already online and {@code isOnline}
     * id {@code true}, then the player is already online and cannot be set online again, so {@code null} is returned 
     * @param client - The Aerospike client instance
     * @param playerId - The player to set online
     * @param isOnline - whether they should be online or not.
     * @return the detail of the player id, if player
     */
    public Player setPlayerOnline(IAerospikeClient client, int playerId, boolean isOnline) {
        WritePolicy writePolicy = client.copyWritePolicyDefault();
        writePolicy.filterExp = isOnline ? Exp.build(Exp.boolBin("isOnline")) : null;
        
        Record record = client.operate(null, getPlayerKey(playerId), 
                Operation.put(new Bin("online", isOnline)),
                Operation.get("id"),
                Operation.get("userName"),
                Operation.get("firstName"),
                Operation.get("lastName"),
                Operation.get("email"),
                Operation.get("shieldExpiry"),
                Operation.get("online"),
                Operation.get("beingAttackedBy"),
                Operation.get("score"));
        return recordToPlayer(record); 
    }

    /** 
     * Set a player to be not online, no shield, not being attacked. if the score is > 0, set the score too
     * @param client - The Aerospike client instance
     * @param playerId - The player to set online
     * @param score - the score to set on the player. Set negative to not set the score.
     */
    public void resetPlayerTo(IAerospikeClient client, int playerId, int score) {
        Bin online = new Bin("online", false);
        Bin shield = new Bin("shieldExpiry", 0);
        Bin beingAttacked = new Bin("beingAttackedBy", "");
        if (score >= 0) {
            client.put(null, getPlayerKey(playerId), online, shield, beingAttacked, new Bin("score", score));    
        }
        else {
            client.put(null, getPlayerKey(playerId), online, shield, beingAttacked);
        }
        
    }

    /**
     * Determine if the player with the passed id can be attacked. Note that this should
     * not be used in real game play, because the result could be stale by the time it's returned.
     * @param client - The Aerospike client instance
     * @param playerId - The player being tested
     * @return - true if the player can be attacked, false otherwise.
     */
    public boolean canAttackPlayerTest(IAerospikeClient client, int playerId) {
        Policy readPolicy = client.copyReadPolicyDefault();
        readPolicy.filterExp = getPlayerFilter();
        readPolicy.failOnFilteredOut = true;
        try  {
            Record result = client.get(readPolicy, getPlayerKey(playerId));
            if (result == null) {
                System.out.println("*** Key " + playerId + " does not exist!");
                return false;
            }
            return true;
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.FILTERED_OUT) {
                return false;
            }
            throw ae;
        }
    }
    
    public int testEligibility(IAerospikeClient client) {
        int playerId = 1;
        int originalScore = client.get(null, getPlayerKey(1)).getInt("score");
        System.out.println("\nTesting eligibility for player " + playerId);
        resetPlayerTo(client, playerId, 590);
        System.out.printf("- Checking player can validly be attacked: %s\n",
                canAttackPlayerTest(client, playerId) ? "PASSED" : "FAILED");
        
        setPlayerOnline(client, playerId, true);
        System.out.printf("- Checking player cannot be attacked when online:: %s\n",
                !canAttackPlayerTest(client, playerId) ? "PASSED" : "FAILED");

        resetPlayerTo(client, playerId, 50);
        System.out.printf("- Checking player cannot be attacked with a low score: %s\n",
                !canAttackPlayerTest(client, playerId) ? "PASSED" : "FAILED");

        resetPlayerTo(client, playerId, 500);
        leaderboard.updatePlayerScore(client, playerId, originalScore, 1234, null);
        return 1234;
    }
    
}
