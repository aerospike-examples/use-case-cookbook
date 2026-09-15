"""Mirrors ../../java-sdk's Parameter<T> - a simple mutable named value that use cases
expose via UseCase.get_params() so users can tune things like record counts without
editing code. Unlike the Java version, ``value`` is just a public attribute - Python has
no need for the reflection trick the Java UseCaseExecutor uses to mutate a "private" field.
"""

from dataclasses import dataclass
from typing import Generic, Optional, TypeVar

T = TypeVar("T")


@dataclass
class Parameter(Generic[T]):
    name: str
    value: T
    description: Optional[str] = None

    def get(self) -> T:
        return self.value
