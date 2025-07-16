# Player Matching
In the gaming arena, there are styles of games which match players against other players. These matches can be online -- matching only other players only for head-to-head matches, or offline where the player is matched with a base (or similar) set up by the other player ahead of time.

These sort of games require matching algorithms to pit players against other players with similar scores. Since scores are typically a measure of ability, two players with similar scores could be reasonably assumed to be of similar abilities, and hence provide a challenging match.

But in popular games there are tens of millions of players, with thousands active at the same time this can be challenging. However, we can use the leaderboard developed in the [Leaderboard Use Case](leaderboard.md) to help with this.

In addition to position on the leaderboard, there are typically other criteria which must be considered. For example:
* Does the player have protection for being attacked like a shield?
* Is the player online? Some games require the opponent to be online, others require them not be online when being attacked.
* Are the being attacked by another player? Most players do not allow one player to be attacked simultaneously by multiple other players.
* Are they a beginner in the game? We could define a beginner as someone with a score < 400 for example.

## Setup
Given how intertwined the leaderboard functionality is with player matching, we're going to re-use the code from the [Leaderboard](leaderboard.md) use case. It's recommended that you are familiar with this material before looking at player matching. 

To this end the `PlayerMatching` class creates an instance of a `Leaderboard` in it's constructor, and uses it when setting up the players. The player setup is also very similar to that in the leaderboard use case:
```java
public PlayerMatching() {
    super();
    this.leaderboard = new Leaderboard();
}

public void setup(IAerospikeClient client, AeroMapper mapper) throws Exception {
    this.leaderboard.setDefaultValues(mapper);
    playerNamespace = mapper.getNamespace(Player.class);
    playerSet = mapper.getSet(Player.class);
    
    client.truncate(null, playerNamespace, playerSet, null);
    client.truncate(null, this.leaderboard.getScoreboardNamespace(), this.leaderboard.getScoreboardSet(), null);
    
    System.out.printf("Generating %,d Players\n", NUM_PLAYERS);
    new Generator(Player.class)
        .generate(1, NUM_PLAYERS, Player.class, player -> {
          mapper.save(player);
          leaderboard.updatePlayerScore(client, player.getId(), -1, player.getScore(), null);
    })
    .monitor();
}
```

## Matching a player
There are several criteria which need to be met to be able to match an attacker with a defender. These are listed above, and three of these can be encoded in a filer expression:
1. The defender cannot be online
2. The defender cannot be an active shield 
3. The defender is not being attacked by someone else
4. The defender's score must be greater than 400

The equivalent expression is:

```java
Exp exp = Exp.and(
        Exp.not(Exp.boolBin("online")),
        Exp.lt(Exp.intBin("shieldExpiry"), Exp.val(new Date().getTime())),
        Exp.or(Exp.eq(Exp.val(""), Exp.stringBin("beingAttackedBy")), Exp.not(Exp.binExists("beingAttackedBy"))),
        Exp.gt(Exp.intBin("score"), Exp.val(400))
    );
```

The only part which is left to find someone to match to attack. If you look at the [Leaderboard](leaderboard.md) we have a function `getScoresAroundPlayer` which finds scores above and below a particular player. This is exactly what we need. Select the players above and below the target player, then apply the filter to each of those players and find one to match against.

```java
List<Player> similarScores = leaderboard.getScoresAroundPlayer(client, attacker.getId(), attacker.getScore(), 20);
// Turn the list of players into a set of keys, exluding this attacker's
Key[] keys = similarScores.stream()
        .filter(player -> player.getId() != attacker.getId())
        .map(player -> new Key(playerNamespace, playerSet, player.getId()))
        .toArray(Key[]::new);

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
```

## Playing the game
Once a player has been selected to be attacked we simulate a game between the attacker and defender. Based on their relative scores, a probabilitistic function is applied to determine the winner, then scores are updated with the victor's score increasing and the loser's score decreasing. (The amount of the score change is calculated using the Elo scoring system, similar to that used in chess tournaments).

Then data model changes are computed and applied to the two players:
```java
Utils.doInTransaction(client, txn -> {
    WritePolicy wp = client.getWritePolicyDefault();
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
```

The attacker loses their shield when the attack and if the defender was defeated they gain a shield for a period of time. When the defending player was selected the flag "beingAttackedBy" was set to the id of the attacker and the filter criteria uses this to prevent multiple attackers attacking the same defender concurrently. This flag must be cleared when the battle is over.

## Running the simulation
Running the simulation uses the `Async` methods as described [here](leaderboard.md#running-the-demonstration). There are only two types of tasks we want to start off this time though:

1. Controlling the player we want to monitor (player1). We will montitor their attacks and results, and show their position on the leaderboard prior to starting and after running the use case. In order to ensure that no-one matches this player, we will set them to be online before the other threads start, and only take them offline when the simulation ends. (Remember that online players cannot be attacked in this use case).

2. Simulating other players attacking. This is done just to put some load on the database, and we can control the number of threads and how frequently they attack. When a player is selected to attack, they are set to be online, thus preventing them from being attacked. When the attack is over, they are set to be offline again.

    Note that one area the simulation doesn't cater for is determining what happens when a player comes online while they are being attacked. Games like "Clash of Clans" handle this scenario by allowing the player to watch the attacks occurring on their bases like, a visually appealing feature for the players, as well as providing a nice solution for how to handle this case. 

```java
Player player1 = setPlayerOnline(client, 1, true);
System.out.printf("Leader board before the games start...\n");
leaderboard.showPlayersAroundPlayer(client, player1.getId(), player1.getScore());

Async.runFor(Duration.ofSeconds(10), (async) -> {
    async.periodic(Duration.ofMillis(100), () -> {
        findPlayerToAttack(client, player1).ifPresent(defender -> {
            playGame(client, player1, defender, true);
        });
    });
    
    async.periodic(Duration.ofMillis(5), 20, () -> {
        // Pick any player besides player one.
        int playerId = async.rand().nextInt(NUM_PLAYERS-1) + 2;
        Player player = setPlayerOnline(client, playerId, true);
        if (player != null) {
            // This player is valid, and not online
            try {
                findPlayerToAttack(client, player).ifPresent(defender -> {
                    playGame(client, player, defender, false);
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
setPlayerOnline(client, player1.getId(), false);
```
