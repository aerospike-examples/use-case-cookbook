"""Mirrors ../../../java-sdk's advancedexpressions/model/Car.java.

This SDK has no annotation-driven object mapper, so the model is a plain dataclass with
its own ``to_bins``/``from_bins`` conversion to/from the dict a record's bins are
represented as.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Car:
    id: int
    make: str
    model: str
    year: int
    body_type: str
    engine_size: float
    color: str
    milage: int
    price: int
    features: list[str] = field(default_factory=list)

    def to_bins(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "make": self.make,
            "model": self.model,
            "year": self.year,
            "bodyType": self.body_type,
            "engineSize": self.engine_size,
            "color": self.color,
            "milage": self.milage,
            "price": self.price,
            "features": self.features,
        }

    @staticmethod
    def from_bins(bins: dict[str, Any]) -> "Car":
        return Car(
            id=bins["id"],
            make=bins["make"],
            model=bins["model"],
            year=bins["year"],
            body_type=bins["bodyType"],
            engine_size=bins["engineSize"],
            color=bins["color"],
            milage=bins["milage"],
            price=bins["price"],
            features=list(bins.get("features") or []),
        )
