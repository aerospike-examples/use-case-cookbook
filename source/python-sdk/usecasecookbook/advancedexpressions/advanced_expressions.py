"""Port of ../../java-sdk's AdvancedExpressions (itself an SDK port of ../../java). Not a
use case per se but a set of techniques for advanced expression usage:

1. Checking whether a value is in a list bin (``"Sunroof" IN features``)
2. Checking whether a bin's value is in a passed-in list (``color IN ["Red","Green","Blue"]``)
3. Performing multiple operations, some depending on others' results, within one write

All three port to AEL exactly as ../../java-sdk has them - including technique 3's single
nested ``let``/``when`` write with ``append(value)`` branches, which requires server-side AEL
compilation (Aerospike 8.2.0+; see ``../README.md``).
"""

import random

from aerospike_async import ListOrderType
from aerospike_sdk import DataSet, QueryHint
from aerospike_sdk.sync import Session

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

    def setup(self, session: Session) -> None:
        session.truncate(CARS)

        print(f"Generating {NUM_CARS:,} Cars")
        for car_id in range(1, NUM_CARS + 1):
            car = _random_car(car_id)
            session.upsert(CARS.id(car.id)).put(car.to_bins()).execute()

    def run(self, session: Session) -> None:
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

        # AEL's append(value) can't create a missing list bin, so it's created via the native
        # CDT builder first; the append and the typed read-back both use AEL.
        key = CARS.id(1)
        session.upsert(key).bin("acc").list_create(ListOrderType.UNORDERED).execute()
        session.upsert(key).bin("acc").upsert_from("$.acc.append(10)").execute()
        session.upsert(key).bin("counter").upsert_from("$.acc.[0]:INT").execute()
        session.upsert(key).bin("acc").remove().execute()

    def _find_cars_with_feature(self, session: Session, feature: str) -> None:
        """Membership check via AEL's ``value in $.bin`` operator."""
        self._show_cars_matching_expression(session, f"'{feature}' in $.features", 10)

    def _find_cars_with_colors(self, session: Session, colors: list[str]) -> None:
        """Same operator, reversed direction: ``$.color in [...]``."""
        color_list = ", ".join(f'"{c}"' for c in colors)
        self._show_cars_matching_expression(session, f"$.color in [{color_list}]", 10)

    def _multiple_commands_in_one_operation(self, session: Session) -> None:
        """Adds all 4 conditional features in ONE write via a 4-level nested AEL ``let``/``when``
        expression, matching ../../java-sdk's ``multipleCommandsInOneOperation``.
        """
        key = CARS.id(1)
        session.upsert(key).bin("color").set_to("Purple").execute()

        print("Record before augmenting:")
        self._print_car(1, self._read_car(session, key).to_bins())

        ael = """
            let (
              color = when ($.color == 'Purple' => $.features.append('Great Color'), default => $.features),
              type  = when ($.bodyType == 'CONVERTIBLE' => (${color}).append('Looks Cool'), default => ${color}),
              power = when ($.engineSize > 5.0 => (${type}).append('Powerful'), default => ${type}),
              age   = when ($.year >= 2020 => (${power}).append('New-ish'), default => ${power})
            ) then (${age})
        """
        session.upsert(key).bin("features").upsert_from(ael).execute()

        print("Record after augmenting:")
        self._print_car(1, self._read_car(session, key).to_bins())

    @staticmethod
    def _read_car(session: Session, key) -> Car:
        stream = session.query(key).execute()
        car = None
        for row in stream:
            if row.is_ok and row.record is not None:
                car = Car.from_bins(row.record.bins)
        stream.close()
        assert car is not None, f"no car found for {key}"
        return car

    def _show_cars_matching_expression(self, session: Session, ael: str, limit: int) -> None:
        # No secondary index on "features"/"color" - this is an intentional full-set scan (the
        # use case's own description notes a secondary index could do this instead), so opt into
        # the primary-index fallback explicitly rather than have the server reject it.
        stream = (
            session.query(CARS)
            .where(ael)
            .with_hint(QueryHint(allow_scans_with_where=True))
            .limit(limit)
            .execute()
        )
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
