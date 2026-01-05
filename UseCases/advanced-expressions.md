# Advanced Expression usage
## Use case
This is a collection of different techniques which can be used to generate very powerful expressions. Aerospike expressions are used for filtering
record operations, XDR operations and index values, and can also be used to write data to the database or return synthetic values based on a combination of bin values. However they are not necessarily intuitive to use and some powerful operations are hidden within them.

This "use case" is more a collection of advanced techniques dealing with expressions than a business use case per-se.

## Data model
In this case the data model is pretty simple, we just want some sample data. This example will use a car:

```java
@AerospikeRecord(namespace = "${demo.namespace:test}", set = "uccb_car")
@GenMagic
@Data
public class Car {
    public enum BodyType { SEDAN, SUV, HATCHBACK, COUPE, CONVERTIBLE, UTE}
    public enum TransmissionType { AUTOMATIC, MANUAL, CVT, DUAL_CLUTCH }
    @AerospikeKey
    @GenExpression("$Key")
    private int id;
    @GenOneOf("Toyota,Holden,Mitsubishi,Kia,Ferrari,Volvo,Audi,Datsun,Suzuki")
    private String make;
    @GenOneOf("Corolla,Commodore,Outlander,Sorento,Swift,488 Spider,XC90")
    private String model;
    @GenNumber(start = 2000, end = 2025)
    private int year;
    private BodyType bodyType;
    @GenNumber(start = 7, end = 80, divisor = 10)
    private double engineSize;
    private String color;
    private int mileage;
    private int price;
    @GenList(minItems = 0, maxItems = 8, stringOptions = "...")
    private List<String> features;
}
```

## Techniques
The following techniques are discussed:
1. Seeing if an item is in a list in the record in the database
2. Seeing if an item in the database is contained in a passed list
3. Performing multiple commands within a single operation.

### 1. Perform an IN operation -- determine if the passed value is a member of a list in the database.

This one is actually pretty easy using the List API. The `getByValue` method determines if an item is in the list represented by a bin, which is exactly what is needed. In this case we don't care about the value nor the position of the item in the list, we only care if it's there or not. Hence we will use a `ListReturnType` of `EXISTS` which will return a boolean value.

```java
Expression exp = Exp.build(ListExp.getByValue(ListReturnType.EXISTS, Exp.val(feature), Exp.listBin("features")));
```

In this case, we want to know if a car has a particular feature, such as a sunroof. This expression can be used in a primary index query, a secondary index query with other filter conditions, and so on. 

#### Using a LIST as a SET
Sometimes the need arises to treat a `List` bin as a set. Using this sort of `IN` operation is often a good indication that the `List` should be a set. Aerospike has no inbuilt `Set` type, but a `List` can be used as a set by passing flags when inserting.

```java
ListPolicy listPolicy =  new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);
client.operate(null, key1, ListOperation.append(
   listPolicy, "list", Value.get("Sunroof")));
```

Here we have specified that the list is `UNORDERED`, although `ORDERED` would work equally well, depending on the use case. The `ADD_UNIQUE` flag specifies that if the value already exists in the list to not insert the value and throw an exception instead. However, attempting to insert an existing element into a set probably shouldn't throw an exception, so `NO_FAIL` is specified to just fail silently instead. 

### 2. Performing an IN operation - determine if a bin value is contained in a passed list.

This is the opposite case of the above. Instead of being passed a single value and seeing if a list in an Aerospike bin contains that value, we have an Aerospike bin which contains a scalar value and want to determine if that value is in the passed list.

For example, our `Car` class has a `color` attribute. Perhaps the user is buying a car and only wants to see cars that are red, green or blue.

We can still solve this using the Aerospike API but it's slightly counter-intuitive. If we look at the `getByValue` API it has:

```java
public static Exp getByValue(int returnType, Exp value, Exp bin, CTX... ctx);
```

The `bin` expression is normally passed using something like `Exp.listBin("list")`, but doesn't need to be. It can be anything of a `List` type. So we can do:

```java
List<String> colors = List.of("Red", "Green", "Blue");
Expression exp = Exp.build(ListExp.getByValue(ListReturnType.EXISTS, Exp.stringBin("color"), Exp.val(colors)));
```

The list of colors is passed into the `bin` parameter and the bin `color` is passed as the `value` parameter, thereby providing the `IN` functionality that is desired.

### 3. Performing multiple commands in one operation.
The `operate` command can perform many different `Operation`s in one call. However, these operations are entirely unrelated and do not have the ability to pass information from one `Operation` to another. Normally this isn't a problem, but there are some situations where it would be useful to be able to do this in one operation. For example:
1. Multi-step operations using `cond` statements which affect the value in the `cond`. Consider the following:
    ```
    if (sizeof (myListBin) > 10) then
        appendItem newItemList to myListBin
        remove first 5 items from myListBin
    end if
    ```
    Note that this is different from 
    ```
    if (sizeof (myListBin) > 10) then
        appendItem newItemList to myListBin
    end if
    if (sizeof (myListBin) > 10) then
        remove first 5 items from myListBin
    end if
    ```
    The latter one can be done with multiple operations, but the semantics of the former one make it difficult to split across different `Operation`s.

2. Complex operations which can incur significant latency. For example, performing a `getByValueRange` call on a `Map` with 100,000 entries. If the commands can be done in a single `Operate` call, then the result of this operation can be cached using the `let` / `def` expressions, and the value re-used multiple times on that operation. However, if the commands cannot be done in a single `Operation` but require multiple `Operation`s then each `Operation` must re-compute this result, even within a single `operate` call. 

Note that a single operation can only affect one bin or form one result, so this technique only works if commands all affect the same bin, as shown in the above example.

For a simple example of this, let's say we want to augment the list of features on a car, using the following pseudo-code:
```
    if color == 'Purple' then
        add 'Great Color' to feature list
    end if;
    if bodyType == 'CONVERTIBLE' then
        add 'Looks Cool' to feature list
    end if
    if engineSize > 5.0 then
        add 'Powerful' to feature list
    end if
    if year >= 2020 then
        add "New-ish" to feature list
    end if
```

***Note:*** this could be achieved using regular operations and doesn't fit into either of the above categories, but it will serve as an example.

The way to achieve this is to use the `let` / `def` expressions, performing each operation inside a portion of the `def`:

```java
client.operate(null, key,
    ExpOperation.write("features", Exp.build(
            Exp.let(
                Exp.def("color", 
                    Exp.cond(
                        Exp.eq(Exp.stringBin("color"), Exp.val("Purple")), 
                        ListExp.append(ListPolicy.Default, Exp.val("Great Color"), Exp.listBin("features")), 
                        Exp.listBin("features")
                    )
                ),
                Exp.def("type", 
                    Exp.cond(
                        Exp.eq(Exp.stringBin("bodyType"), Exp.val("CONVERTIBLE")), 
                        ListExp.append(ListPolicy.Default, Exp.val("Looks Cool"), Exp.var("color")),
                        Exp.var("color")
                    )
                ),
                Exp.def("power", 
                    Exp.cond(
                        Exp.gt(Exp.floatBin("engineSize"), Exp.val(5.0)), 
                        ListExp.append(ListPolicy.Default, Exp.val("Powerful"), Exp.var("type")), 
                        Exp.var("type")
                    )
                ),
                Exp.def("age", 
                    Exp.cond(
                        Exp.ge(Exp.intBin("year"), Exp.val(2020)), 
                        ListExp.append(ListPolicy.Default, Exp.val("New-ish"), Exp.var("power")), 
                        Exp.var("power")
                    )
                ),
                Exp.var("age")
            )
        ), ExpWriteFlags.DEFAULT)
    );
```

In this case we want to update the `features` bin. Once a bin has been read, it is immutable for the duration of the expression evaluation so we cannot use `ListExp.append(ListPolicy.Default, Exp.val("Great Color"), Exp.listBin("features"))` and expect it to modify the contents of the `features` bin; it won't. However, it will return a copy of the modified list with the items attached. 

Hence, the first stanza
```java
Exp.def("color", 
    Exp.cond(
        Exp.eq(Exp.stringBin("color"), Exp.val("Purple")), 
        ListExp.append(ListPolicy.Default, Exp.val("Great Color"), Exp.listBin("features")), 
        Exp.listBin("features")
    )
)
```
will create a new variable `color` which contains either the contents of the list bin `features` with "Great Color" appended to it (if the color is purple), or the contents of the original list otherwise. 

The next stanza
```java
Exp.def("type", 
    Exp.cond(
        Exp.eq(Exp.stringBin("bodyType"), Exp.val("CONVERTIBLE")), 
        ListExp.append(ListPolicy.Default, Exp.val("Looks Cool"), Exp.var("color")),
        Exp.var("color")
    )
),
```
again performs a conditional check, but in this case either adds "Looks Cool" to the list currently stored in the `color` variable if the car is a convertible, or returns the unmodified `color` variable. Note that it does not use the list bin of `features` as this will lose any changes done in the first stanza. 

After the four conditions have been evaluated and the action performed, the result is in the `age` variable, so this is written back to the `features` bin using `ExpOperation.write(...)`.
