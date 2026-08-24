package com.aerospike.examples.advancedexpressions;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.TypedDataSet;
import com.aerospike.client.sdk.TypedKey;
import com.aerospike.client.sdk.TypedRecordStream;
import com.aerospike.client.sdk.cdt.ListOrder;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.ListExp;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.advancedexpressions.model.Car;

/**
 * SDK port of the legacy {@code AdvancedExpressions} (see ../../java). Not a use case per se but
 * a set of techniques for advanced expression usage:
 * <ol>
 *   <li>Checking whether a value is in a list bin ({@code "Sunroof" IN features})</li>
 *   <li>Checking whether a bin's value is in a passed-in list ({@code color IN ["Red","Green","Blue"]})</li>
 *   <li>Performing multiple operations, some depending on others' results, within one operate call</li>
 * </ol>
 */
public class AdvancedExpressions implements UseCase {

    private static final int NUM_CARS = 10_000;

    private static final String[] MAKES = {"Toyota", "Holden", "Mitsubishi", "Kia", "Ferrari", "Volvo", "Audi", "Datsun", "Suzuki"};
    private static final String[] MODELS = {"Corolla", "Commodore", "Outlander", "Sorento", "Swift", "488 Spider", "XC90"};
    private static final String[] COLORS = {"Red", "Green", "Blue", "White", "Black", "Silver", "Purple"};
    private static final String[] ALL_FEATURES = {
            "Bluetooth Connectivity", "USB Charging Ports", "Apple CarPlay", "Android Auto", "Heated Seats",
            "Ventilated Seats", "Sunroof", "Panoramic Roof", "Navigation System", "Keyless Entry",
            "Push Button Start", "Remote Start", "Adaptive Cruise Control", "Blind Spot Monitoring",
            "Lane Departure Warning", "Automatic Emergency Braking", "Parking Sensors", "Rearview Camera",
            "360-Degree Camera", "Leather Upholstery", "Wireless Charging", "Heads-Up Display",
            "Premium Sound System", "LED Headlights", "Rain-Sensing Wipers", "Heated Steering Wheel",
            "Power Liftgate", "Roof Rails", "Tow Package", "Ambient Interior Lighting"
    };

    @Override
    public String getName() {
        return "Advanced Expressions";
    }

    @Override
    public String getDescription() {
        return "A set of techniques showing advanced usage of expressions. This is not a use case per-se but rather a set "
                + "of techniques that can be used in a use case. Current examples include:\n"
                + "1. Seeing if an item is in a list in the record in the database\n"
                + "2. Seeing if an item in the database is contained in a passed list\n"
                + "3. Performing multiple operations that return information in a single operation within one operate command.";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/advanced-expressions.md";
    }

    private final TypedDataSet<Car> cars =
            TypedDataSet.of(System.getProperty("demo.namespace", "test"), "uccb_car", Car.class);

    private Car randomCar(int id) {
        int numFeatures = ThreadLocalRandom.current().nextInt(0, 9);
        List<String> features = ThreadLocalRandom.current().ints(0, ALL_FEATURES.length)
                .distinct().limit(numFeatures).mapToObj(i -> ALL_FEATURES[i]).toList();
        Car.BodyType[] bodyTypes = Car.BodyType.values();
        return new Car(id,
                MAKES[ThreadLocalRandom.current().nextInt(MAKES.length)],
                MODELS[ThreadLocalRandom.current().nextInt(MODELS.length)],
                ThreadLocalRandom.current().nextInt(2000, 2025),
                bodyTypes[ThreadLocalRandom.current().nextInt(bodyTypes.length)].name(),
                ThreadLocalRandom.current().nextInt(7, 80) / 10.0,
                COLORS[ThreadLocalRandom.current().nextInt(COLORS.length)],
                ThreadLocalRandom.current().nextInt(0, 200_000),
                ThreadLocalRandom.current().nextInt(5_000, 150_000),
                features);
    }

    @Override
    public void setup(Session session) throws Exception {
        session.truncate(cars);

        System.out.printf("Generating %,d Cars%n", NUM_CARS);
        for (int id = 1; id <= NUM_CARS; id++) {
            session.upsert(cars).object(randomCar(id)).execute();
        }
    }

    @Override
    public void run(Session session) throws Exception {
        System.out.println("Find 10 cars which have Sunroofs. The features of the car are stored in the 'features' bin, so this is "
                + "effectively doing:\n"
                + "      \"Sunroof\" IN features\n"
                + "Note that this could be done using a secondary index too.");

        findCarsWithFeature(session, "Sunroof");

        System.out.println("\n\n");
        System.out.println("Find 10 cars whose color is any of Red, Green or Blue. Since Aerospike does not have an IN operation, "
                + "this will use a list operation to perform this. Effectively this is doing:\n"
                + "      color IN [\"Red\", \"Green\", \"Blue\"]");
        findCarsWithColors(session, List.of("Red", "Green", "Blue"));

        multipleCommandsInOneOperation(session);

        // The "acc" list-append is written as AEL (verified against a live cluster to behave
        // identically to ListExp.append) - per Tim's review comment to prefer AEL where possible.
        // The "counter" read-back is NOT converted: every AEL index-read syntax tried against a live
        // cluster ($.acc.getByIndex(0), $.acc[0], $.acc.{0}, $.acc.{0}.getValue()) threw the same
        // server-side "Parameter error" that blocked Leaderboard's map-index read (see that class's
        // getScoresAroundPlayer javadoc) - single-element list/map index reads via AEL appear to be
        // an unsupported composition on this build, not just a syntax guess away. Kept on the
        // proven ListExp.getByIndex Exp form; worth revisiting with correct syntax from Tim.
        TypedKey<Car> key = cars.id(1);
        session.upsert(key)
                .bin("acc").listCreate(ListOrder.UNORDERED)
                .bin("acc").upsertFrom("$.acc.append(10)")
                .bin("counter").upsertFrom(ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.val(0), Exp.listBin("acc")))
                .bin("acc").remove()
                .execute();
    }

    /**
     * Kept as {@code ListExp.getByValue(EXISTS, ...)} rather than an AEL string per Tim's review
     * comment: tried two AEL translations of "value exists in a list bin" against a live cluster
     * ({@code $.features.contains('Sunroof')} - server-side {@code Parameter error}; {@code
     * $.features.{='Sunroof'}.count() > 0} - parses fine but silently returns 0 matches instead of
     * the correct 10) and neither is safe to ship without an authoritative AEL grammar reference
     * for list-membership/selector syntax. Worth revisiting with the correct syntax from Tim.
     */
    private void findCarsWithFeature(Session session, String feature) {
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS, Exp.val(feature), Exp.listBin("features"));
        showCarsMatchingExpression(session, exp, 10);
    }

    /** Same rationale as {@link #findCarsWithFeature} - kept as Exp, not converted to AEL. */
    private void findCarsWithColors(Session session, List<String> colors) {
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS, Exp.stringBin("color"), Exp.val(colors));
        showCarsMatchingExpression(session, exp, 10);
    }

    /**
     * Using just ONE operation, adds:
     * <ul>
     *   <li>a feature "Great Color" if the color is Purple</li>
     *   <li>"Looks Cool" if the body type is CONVERTIBLE</li>
     *   <li>"Powerful" if the engine is over 5L</li>
     *   <li>"New-ish" if the year is 2020 or later</li>
     * </ul>
     */
    private void multipleCommandsInOneOperation(Session session) {
        TypedKey<Car> key = cars.id(1);
        session.upsert(key).bin("color").setTo("Purple").execute();
        System.out.println("Record before augmenting:");
        showCar(1, session.query(key).execute().getFirstRecord());

        // Written as AEL rather than nested Exp.let/Exp.def/Exp.cond builder calls - much easier to
        // read once the nesting gets this deep, and it's the same expression the server evaluates.
        session.upsert(key)
                .bin("features").upsertFrom("""
                        let (
                          color = when (
                            $.color == 'Purple' => $.features.append('Great Color'),
                            default => $.features
                          ),
                          type = when (
                            $.bodyType == 'CONVERTIBLE' => (${color}).append('Looks Cool'),
                            default => ${color}
                          ),
                          power = when (
                            $.engineSize > 5.0 => (${type}).append('Powerful'),
                            default => ${type}
                          ),
                          age = when (
                            $.year >= 2020 => (${power}).append('New-ish'),
                            default => ${power}
                          )
                        ) then (${age})
                        """)
                .execute();
        System.out.println("Record after augmenting:");
        showCar(1, session.query(key).execute().getFirstRecord());
    }

    private void showCarsMatchingExpression(Session session, Exp exp, int limit) {
        AtomicInteger count = new AtomicInteger();
        try (TypedRecordStream<Car> stream = session.query(cars).where(exp).limit(limit).execute()) {
            stream.forEach(result -> {
                if (result.isOk()) {
                    showCar(count.incrementAndGet(), result.recordOrThrow());
                }
            });
        }
    }

    private void showCar(int count, Record rec) {
        System.out.printf("%nCar %d (id: %d)%n", count, rec.getInt("id"));
        System.out.printf("\tMake:        %s%n", rec.getString("make"));
        System.out.printf("\tModel:       %s%n", rec.getString("model"));
        System.out.printf("\tYear:        %s%n", rec.getInt("year"));
        System.out.printf("\tColor:       %s%n", rec.getString("color"));
        System.out.printf("\tType:        %s%n", rec.getString("bodyType"));
        System.out.printf("\tEngine Size: %s%n", rec.getDouble("engineSize"));
        System.out.printf("\tFeatures:    %s%n", rec.getList("features"));
    }
}
