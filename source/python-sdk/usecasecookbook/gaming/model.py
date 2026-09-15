"""Mirrors ../../../java-sdk's gaming/model/Player.java."""

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class Player:
    id: int
    user_name: str
    first_name: str
    last_name: str
    email: str
    shield_expiry: int
    online: bool
    being_attacked_by: str
    score: int

    def to_bins(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "userName": self.user_name,
            "firstName": self.first_name,
            "lastName": self.last_name,
            "email": self.email,
            "shieldExpiry": self.shield_expiry,
            "online": self.online,
            "beingAttackedBy": self.being_attacked_by,
            "score": self.score,
        }

    @staticmethod
    def from_bins(bins: Dict[str, Any]) -> "Player":
        return Player(
            id=bins["id"],
            user_name=bins["userName"],
            first_name=bins["firstName"],
            last_name=bins["lastName"],
            email=bins["email"],
            shield_expiry=bins["shieldExpiry"],
            online=bins["online"],
            being_attacked_by=bins["beingAttackedBy"],
            score=bins["score"],
        )
