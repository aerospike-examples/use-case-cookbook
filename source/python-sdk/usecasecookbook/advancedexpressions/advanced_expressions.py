"""Port of ../../java-sdk's AdvancedExpressions (itself an SDK port of ../../java). Not a
use case per se but a set of techniques for advanced expression usage:

1. Checking whether a value is in a list bin (``"Sunroof" IN features``)
2. Checking whether a bin's value is in a passed-in list (``color IN ["Red","Green","Blue"]``)
3. Performing multiple operations, some depending on others' results, within one write

Techniques 1 and 2 port to AEL exactly as ../../java-sdk has them. Technique 3 does not -
see :meth:`AdvancedExpressions.multiple_commands_in_one_operation` for why.
"""

import random

from aerospike_async import ListOrderType
from aerospike_sdk import DataSet, SyncSession

from usecasecookbook import config
from usecasecookbook.advancedexpressions.model import Car
from usecasecookbook.use_case import UseCase

NUM_CARS = 10_000

MAKES = ["Toyota", "Holden", "Mitsubishi", "Kia", "Ferrari", "Volvo", "Audi", "Datsun", "Suzuki"]
MODELS = ["Corolla", "Commodore", "Outlander", "Sorento", "Swift", "488 Spider", "XC90"]
COLORS = ["Red", "Green", "Blue", "White", "Black", "Silver", "Purple"]
BODY_TYPES = ["SEDAN", "SUV", "HATCHBACK", "COUPE", "CONVERTIBLE", "UTE"]
ALL_FEATURES = [
    "Bluetooth Connectivity", "USB Charging Ports", "Apple CarPlay", "Android Auto", "Heated Seats",
    "Ventilated Seats", "Sunroof", "Panoramic Roof", "Navigation System", "Keyless Entry",
    "Push Button Start", "Remote Start", "Adaptive Cruise Control", "Blind Spot Monitoring",
    "Lane Departure Warning", "Automatic Emergency Braking", "Parking Sensors", "Rearview Camera",
    "360-Degree Camera", "Leather Upholstery", "Wireless Charging", "Heads-Up Display",
    "Premium Sound System", "LED Headlights", "Rain-Sensing Wipers", "Heated Steering Wheel",
    "Power Liftgate", "Roof Rails", "Tow Package", "Ambient Interior Lighting",
]

CARS = DataSet.of(config.NAMESPACE, "uccb_car")


def _random_car(car_id: int) -> Car:
    num_features = random.randint(0, 8)
    features = random.sample(ALL_FEATURES, num_features)
    return Car(
        id=car_id,
        make=random.choice(MAKES),
        model=random.choice(MODELS),
        year=random.randint(2000, 2024),
        body_type=random.choice(BODY_TYPES),
        engine_size=random.randint(7, 79) / 10.0,
        color=random.choice(COLORS),
        milage=random.randint(0, 199_999),
        price=random.randint(5_000, 149_999),
        features=features,
    )


class AdvancedExpressions(UseCase):
    def get_name(self) -> str:
        return "Advanced Expressions"

    def get_description(self) -> str:
        return (
            "A set of techniques showing advanced usage of expressions. This is not a use case per-se but rather a "
            "set of techniques that can be used in a use case. Current examples include:\n"
            "1. Seeing if an item is in a list in the record in the database\n"
            "2. Seeing if an item in the database is contained in a passed list\n"
            "3. Performing multiple operations that return information in a single operation within one operate command."
        )

    def get_reference(self) -> str:
        return "https://github.com/aerospike-examples/use-case-cookbook/blob/main/UseCases/advanced-expressions.md"

    def setup(self, session: SyncSession) -> None:
        session.truncate(CARS)

        print(f"Generating {NUM_CARS:,} Cars")
        for car_id in range(1, NUM_CARS + 1):
            car = _random_car(car_id)
            session.upsert(CARS.id(car.id)).put(car.to_bins()).execute()

    def run(self, session: SyncSession) -> None:
        print(
            "Find 10 cars which have Sunroofs. The features of the car are stored in the 'features' bin, so this is "
            "effectively doing:\n"
            "      \"Sunroof\" IN features\n"
            "Note that this could be done using a secondary index too."
        )
        self._find_cars_with_feature(session, "Sunroof")

        print("\n\n")
        print(
            "Find 10 cars whose color is any of Red, Green or Blue. Since Aerospike does not have an IN operation, "
            "this will use a list operation to perform this. Effectively this is doing:\n"
            "      color IN [\"Red\", \"Green\", \"Blue\"]"
        )
        self._find_cars_with_colors(session, ["Red", "Green", "Blue"])

        self._multiple_commands_in_one_operation(session)

        # A scalar path read needs an explicit type cast - the server can't infer the type of
        # a single-element read from the path alone. The canonical AEL grammar spells this as
        # a ":TYPE" path suffix (e.g. "$.acc.[0]:INT", same as ../../java-sdk uses) - but this
        # SDK's currently-published package (aerospike-sdk==0.9.0a5) parses AEL strings with
        # its own bundled, client-side grammar rather than sending them to the server for
        # compilation, and that bundled grammar predates the ":TYPE" suffix convention
        # entirely: EVERY ":TYPE" suffix fails to parse against it, not just this one
        # (confirmed empirically: "$.acc.[0]:INT" raises AelParseException: line 1:9 mismatched
        # input ':' expecting <EOF>). ".get(type: INT)" is that older, still-published
        # package's own equivalent mechanism, and is what actually works against the version
        # of this SDK anyone gets from `pip install aerospike-sdk` today. The SDK's actively
        # developed (but not yet publicly released) branch has since moved AEL compilation
        # server-side and adopted the canonical ":TYPE" syntax - this line should switch to it
        # once that lands in a published release.
        #
        # append(10) can't be expressed in AEL on this package either way - its grammar only
        # accepts a bare, argument-less "append()" path function ("$.acc.append(10)" raises
        # AelParseException: line 1:12 mismatched input '(' expecting '()'), and the canonical
        # grammar's write-shaped path terminals (§13 of the canonical AEL reference) aren't
        # reachable from string AEL in this package regardless of the type-suffix issue above -
        # so the append itself uses the native CDT builder; only the typed read-back into
        # "counter" uses AEL.
        key = CARS.id(1)
        session.upsert(key) \
            .bin("acc").list_create(ListOrderType.UNORDERED) \
            .bin("acc").list_append(10) \
            .execute()
        session.upsert(key).bin("counter").upsert_from("$.acc.[0].get(type: INT)").execute()
        session.upsert(key).bin("acc").remove().execute()

    def _find_cars_with_feature(self, session: SyncSession, feature: str) -> None:
        """Membership check via AEL's ``value in $.bin`` operator."""
        self._show_cars_matching_expression(session, f"'{feature}' in $.features", 10)

    def _find_cars_with_colors(self, session: SyncSession, colors: list[str]) -> None:
        """Same operator, reversed direction: ``$.color in [...]``."""
        color_list = ", ".join(f'"{c}"' for c in colors)
        self._show_cars_matching_expression(session, f"$.color in [{color_list}]", 10)

    def _multiple_commands_in_one_operation(self, session: SyncSession) -> None:
        """../../java-sdk adds all 4 conditional features in ONE write via a 4-level nested
        AEL ``let``/``when`` expression, each branch appending a string with
        ``$.features.append('...')``. The canonical AEL grammar supports exactly this
        (``$.tags.append('new')`` is a documented example of a write-shaped path terminal in
        the canonical AEL grammar's path-write-terminal section) and it's what ../../java-sdk
        uses. It's not
        portable to the currently-published ``aerospike-sdk==0.9.0a5`` package, though: that
        package's bundled client-side AEL parser only recognizes a bare, argument-less
        ``append()`` path function (confirmed empirically - passing an argument raises the same
        ``AelParseException`` as the ``acc``/``counter`` demo above) - this is the same
        stale-local-grammar issue documented there, not a difference in what canonical AEL
        itself supports.

        The nested ``let``/``when`` composition to *evaluate* the 4 conditions works fine
        (verified against a live cluster: color/bodyType/engineSize/year all correctly
        combined through nested let-bound variables) - only the "and append the result to a
        list bin" write half is the gap. So this reads the car's 4 scalar bins, evaluates
        the same 4 conditions in Python, and appends the qualifying feature names with one
        native ``list_append_items`` write - one read + one write instead of ../../java-sdk's
        single write, but the same conditional-features result.
        """
        key = CARS.id(1)
        session.upsert(key).bin("color").set_to("Purple").execute()

        car = self._read_car(session, key)
        print("Record before augmenting:")
        self._print_car(1, car.to_bins())

        features_to_add = []
        if car.color == "Purple":
            features_to_add.append("Great Color")
        if car.body_type == "CONVERTIBLE":
            features_to_add.append("Looks Cool")
        if car.engine_size > 5.0:
            features_to_add.append("Powerful")
        if car.year >= 2020:
            features_to_add.append("New-ish")

        if features_to_add:
            session.upsert(key).bin("features").list_append_items(features_to_add).execute()

        print("Record after augmenting:")
        self._print_car(1, self._read_car(session, key).to_bins())

    @staticmethod
    def _read_car(session: SyncSession, key) -> Car:
        stream = session.query(key).execute()
        car = None
        for row in stream:
            if row.is_ok and row.record is not None:
                car = Car.from_bins(row.record.bins)
        stream.close()
        assert car is not None, f"no car found for {key}"
        return car

    def _show_cars_matching_expression(self, session: SyncSession, ael: str, limit: int) -> None:
        stream = session.query(CARS).where(ael).limit(limit).execute()
        count = 0
        for row in stream:
            if row.is_ok and row.record is not None:
                count += 1
                self._print_car(count, row.record.bins)
        stream.close()

    @staticmethod
    def _print_car(count: int, bins: dict) -> None:
        print()
        print(f"Car {count} (id: {bins['id']})")
        print(f"\tMake:        {bins['make']}")
        print(f"\tModel:       {bins['model']}")
        print(f"\tYear:        {bins['year']}")
        print(f"\tColor:       {bins['color']}")
        print(f"\tType:        {bins['bodyType']}")
        print(f"\tEngine Size: {bins['engineSize']}")
        print(f"\tFeatures:    {bins['features']}")
