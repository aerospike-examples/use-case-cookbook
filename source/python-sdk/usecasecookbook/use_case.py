"""Mirrors ../../java-sdk's UseCase interface.

There is no annotation-driven object mapper in this SDK (unlike
aerospike-sdk-mapper-java on the Java SDK side), so ``setup``/``run`` are handed a plain
``Session`` only - each use case builds its own ``DataSet``s directly (see each use
case's module for its namespace/set names, taken from :mod:`usecasecookbook.config`).
"""

from abc import ABC, abstractmethod

from aerospike_sdk.sync import Session

from usecasecookbook.parameter import Parameter


class UseCase(ABC):
    @abstractmethod
    def get_name(self) -> str:
        """A brief but descriptive name for this use case."""

    @abstractmethod
    def get_description(self) -> str:
        """What this use case does and how - this text is searchable."""

    @abstractmethod
    def get_reference(self) -> str:
        """URL of the UseCases/*.md doc fully documenting this use case."""

    def get_tags(self) -> list[str]:
        return []

    def get_params(self) -> list[Parameter]:
        return []

    @abstractmethod
    def setup(self, session: Session) -> None:
        """Truncate the set(s) and (re)generate this use case's data.

        Run whenever the use case is selected; does not necessarily reflect the
        business logic ``run`` demonstrates.
        """

    @abstractmethod
    def run(self, session: Session) -> None:
        """Execute the use case and print results to the console.

        Results should be self-explanatory, or documented at ``get_reference()``.
        """
