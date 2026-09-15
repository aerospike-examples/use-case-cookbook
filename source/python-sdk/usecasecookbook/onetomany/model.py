"""Mirrors ../../../java-sdk's onetomany/model/{Agent,Listing}.java.

No object mapper exists in this SDK - each model is a plain dataclass with hand-written
``to_bins``/``from_bins`` (see ../setup/model.py for the established pattern).
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class Agent:
    agent_id: int
    first_name: str
    last_name: str
    email: str
    phone_num: str
    reg_date: datetime

    def to_bins(self) -> Dict[str, Any]:
        return {
            "agentId": self.agent_id,
            "firstName": self.first_name,
            "lastName": self.last_name,
            "email": self.email,
            "phoneNum": self.phone_num,
            "regDate": self.reg_date.timestamp(),
        }

    @staticmethod
    def from_bins(bins: Dict[str, Any]) -> "Agent":
        return Agent(
            agent_id=bins["agentId"],
            first_name=bins["firstName"],
            last_name=bins["lastName"],
            email=bins["email"],
            phone_num=bins["phoneNum"],
            reg_date=datetime.fromtimestamp(bins["regDate"]),
        )


@dataclass
class Listing:
    id: str
    line1: str
    line2: str
    city: str
    state: str
    zip_code: str
    url: str
    date_listed: datetime
    agent_id: int
    description: str

    def to_bins(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "line1": self.line1,
            "line2": self.line2,
            "city": self.city,
            "state": self.state,
            "zipCode": self.zip_code,
            "url": self.url,
            "dateListed": self.date_listed.timestamp(),
            "agentId": self.agent_id,
            "description": self.description,
        }

    @staticmethod
    def from_bins(bins: Dict[str, Any]) -> "Listing":
        return Listing(
            id=bins["id"],
            line1=bins["line1"],
            line2=bins["line2"],
            city=bins["city"],
            state=bins["state"],
            zip_code=bins["zipCode"],
            url=bins["url"],
            date_listed=datetime.fromtimestamp(bins["dateListed"]),
            agent_id=bins["agentId"],
            description=bins["description"],
        )
