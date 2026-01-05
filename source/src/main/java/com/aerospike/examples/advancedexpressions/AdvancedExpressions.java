package com.aerospike.examples.advancedexpressions;

import java.util.List;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Exp.Type;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.examples.UseCase;
import com.aerospike.examples.advancedexpressions.model.Car;
import com.aerospike.examples.gaming.model.Player;
import com.aerospike.generator.Generator;
import com.aerospike.mapper.tools.AeroMapper;

public class AdvancedExpressions implements UseCase {

    private static final int NUM_CARS = 10_000;
    
    private String carNamespace;
    private String carSet;

    @Override
    public String getName() {
        return "Advanced Expressions";
    }

    @Override
    public String getDescription() {
        return "A set of techniques showing advanced usage of expressions. This is not a use case per-se but rather a set of techniques"
                + "that can be used in a use case. Current examples include:\n"
                + "1. Seeing if an item is in a list in the record in the database\n"
                + "2. Seeing if an item in the database is contained in a passed list\n"
                + "3. Performing multiple operations that return information is a single operation within one operate command.";
    }

    @Override
    public String getReference() {
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/advanced-expressions.md";
    }

    @Override
    public String[] getTags() {
        return new String[] {"Expressions", "Map Expressions", "ListExpressions" };
    }

    public void setDefaultValues(AeroMapper mapper) {
        this.carNamespace = mapper.getNamespace(Car.class);
        this.carSet = mapper.getSet(Car.class);
    }

    @Override
    public void setup(IAerospikeClient client, AeroMapper mapper) throws Exception {
        setDefaultValues(mapper);
        client.truncate(null, carNamespace, carSet, null); 
        
        System.out.printf("Generating %,d Cars\n", NUM_CARS);
        new Generator(Player.class)
            .generate(1, NUM_CARS, Car.class, mapper::save)
            .monitor();
    }

    @Override
    public void run(IAerospikeClient client, AeroMapper mapper) throws Exception {
        System.out.println("Find 10 cars which have Sunroofs. The features of the car are stored in the 'features' bin, so this is "
                + "effectively doing:\n"
                + "      \"Sunroof\" IN features\n"
                + "Note that this could be done using a secondary index too.");
        
        findCarsWithFeature(client, "Sunroof");
        
        System.out.println("\n\n");
        System.out.println("Find 10 cars whose color is any of Red, Green or Blue. Since Aerospike does not have an IN operation,"
                + "this will use a list operaiton to perform this. Effective this is doing:\n"
                + "      color IN [\"Red\", \"Green\", \"Blue\"");
        findCarsWithColors(client, List.of("Red", "Green", "Blue"));
        
        multipleCommandsInOneOperation(client);
        client.operate(null, new Key(carNamespace, carSet, 1),
                ListOperation.create("acc", ListOrder.UNORDERED, true),
                ExpOperation.write("acc", Exp.build(ListExp.append(ListPolicy.Default, Exp.val(10), Exp.listBin("acc"))), ExpReadFlags.DEFAULT),
                ExpOperation.write("counter", Exp.build(ListExp.getByIndex(ListReturnType.VALUE, Type.INT, Exp.val(0), Exp.listBin("acc"))), 0),
                Operation.put(Bin.asNull("acc")));
    }
    
    private void findCarsWithFeature(IAerospikeClient client, String feature) {
        Expression exp = Exp.build(ListExp.getByValue(ListReturnType.EXISTS, Exp.val(feature), Exp.listBin("features")));
        showCarsMatchingExpression(client, exp, 10);
    }
    
    private void findCarsWithColors(IAerospikeClient client, List<String> colors) {
        Expression exp = Exp.build(ListExp.getByValue(ListReturnType.EXISTS, Exp.stringBin("color"), Exp.val(colors)));
        showCarsMatchingExpression(client, exp, 10);
    }
    
    private void multipleCommandsInOneOperation(IAerospikeClient client) {
        Key key = new Key(carNamespace, carSet, 1);
        client.put(null, key, new Bin("color", "Purple"));
        System.out.println("Record before augmenting:");
        showCar(1, client.get(null, key));
        
        // Using just ONE operation, add:
        // - a feature "Great Color" if the color is Purple
        // - "Looks cool" if the type is CONVERTIBLE,
        // - "Powerful" if engine > 5L
        // - "New-ish" if the year is on or after 2020
        client.operate(null, key,
            ExpOperation.write("features", Exp.build(
                    Exp.let(
                        Exp.def("color", Exp.cond(Exp.eq(Exp.stringBin("color"), Exp.val("Purple")), ListExp.append(ListPolicy.Default, Exp.val("Great Color"), Exp.listBin("features")), Exp.listBin("features"))),
                        Exp.def("type", Exp.cond(Exp.eq(Exp.stringBin("bodyType"), Exp.val("CONVERTIBLE")), ListExp.append(ListPolicy.Default, Exp.val("Looks Cool"), Exp.var("color")), Exp.var("color"))),
                        Exp.def("power", Exp.cond(Exp.gt(Exp.floatBin("engineSize"), Exp.val(5.0)), ListExp.append(ListPolicy.Default, Exp.val("Powerful"), Exp.var("type")), Exp.var("type"))),
                        Exp.def("age", Exp.cond(Exp.ge(Exp.intBin("year"), Exp.val(2020)), ListExp.append(ListPolicy.Default, Exp.val("New-ish"), Exp.var("power")), Exp.var("power"))),
                        Exp.var("age")
                    )
                ), ExpWriteFlags.DEFAULT)
            );
        System.out.println("Record after augmenting:");
        showCar(1, client.get(null, key));
    }
    
    /**
     * Each operation in an {@code operate} command can affect only one bin (apart from the "get" operation which is special). But there are times
     * when we want continuity of data between them. For example, consider the feature list of a car. Someone adds a new feature onto the list, but
     * the use case calls for the feature list to be finitely bounded, let's say with 10 features. When a feature is added which is the 11th or greater
     * feature, the list is truncated to the first half, then the remaining items returned in an "overflow" bin.<p/>
     * <p/>
     * So the steps needed are:
     * <ol>
     * <li>Add the new feature to the list</li>
     * <li>If there are > 10 then remove last 1/2 of the list</li>
     * <li>
     * </ol>
     * @param client
     */
    private void passingInformationBetweenOperations(IAerospikeClient client) {
        Key key = new Key(carNamespace, carSet, 1);
        showCar(1, client.get(null, key));
        client.put(null, key, new Bin("list", List.of("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p")));
        System.out.println(client.operate(null, key,
                ListOperation.append("list", Value.get("q")),
                Operation.put(new Bin("_temp_", List.of())),
                ListOperation.create("_temp_", ListOrder.UNORDERED, true),
                ExpOperation.write("_temp_", Exp.build(ListExp.append(ListPolicy.Default, ListExp.size(Exp.listBin("list")), Exp.listBin("_temp_"))), ListWriteFlags.DEFAULT),

                // If there are now > 10 items, copy half the items to the list
                ExpOperation.write("_temp_", Exp.build(
                        ListExp.append(ListPolicy.Default, 
                            Exp.cond(
                                Exp.gt(ListExp.size(Exp.listBin("list")), Exp.val(10)), 
                                ListExp.getByIndexRange(ListReturnType.VALUE, 
                                        Exp.div(ListExp.size(Exp.listBin("list")), Exp.val(2)), 
                                        Exp.listBin("list")),
                                Exp.val(List.of())
                            ),
                            Exp.listBin("_temp_")))
                    , ExpWriteFlags.DEFAULT),
                
                // If there were greater than 10 items after we insert this one, remove extra items
                ExpOperation.write("list", 
                        Exp.build(
                                Exp.let(
                                        Exp.def("origSize", ListExp.getByIndex(ListReturnType.VALUE, Type.INT, Exp.val(0), Exp.listBin("_temp_"))),
                                        Exp.cond(
                                                Exp.gt(Exp.var("origSize"), Exp.val(10)),
                                                ListExp.getByIndexRange(ListReturnType.VALUE, Exp.val(0), Exp.div(Exp.var("origSize"), Exp.val(2)), Exp.listBin("list")),
                                                Exp.unknown()
                                        )
                                )
                        ),
                        ExpWriteFlags.EVAL_NO_FAIL),
                
                ExpOperation.read("overflow", Exp.build(ListExp.getByIndex(ListReturnType.VALUE, Type.LIST, Exp.val(1), Exp.listBin("_temp_"))), ExpReadFlags.DEFAULT),
                Operation.put(Bin.asNull("_temp_"))
        ));
        System.out.println("result = " + client.get(null, key));
        
    }
    private void showCarsMatchingExpression(IAerospikeClient client, Expression exp, int limit) {
        Statement stmt = new Statement();
        stmt.setMaxRecords(limit);
        stmt.setNamespace(carNamespace);
        stmt.setSetName(carSet);
        QueryPolicy qp = client.copyQueryPolicyDefault();
        qp.filterExp = exp;
        
        try (RecordSet rs = client.query(qp, stmt)) {
            
            int count = 0;
            while (rs.next()) {
                showCar(++count, rs.getRecord());
            }
        }
    }
    
    private void showCar(int count, Record rec) {
        System.out.printf("\nCar %d (id: %d)\n", count, rec.getInt("id"));
        System.out.printf("\tMake:        %s\n", rec.getString("make"));
        System.out.printf("\tModel:       %s\n", rec.getString("model"));
        System.out.printf("\tYear:        %s\n", rec.getInt("year"));
        System.out.printf("\tColor:       %s\n", rec.getString("color"));
        System.out.printf("\tType:        %s\n", rec.getString("bodyType"));
        System.out.printf("\tEngine Size: %s\n", rec.getDouble("engineSize"));
        System.out.printf("\tFeatures:    %s\n", rec.getList("features"));
    }

}
