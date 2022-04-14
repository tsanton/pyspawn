from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pyspawn._graph.relationship import Relationship

from dataclasses import dataclass
from typing import Set


@dataclass(frozen=False)
class Table:
    """Table class"""
    schema: str
    table_name: str


    def __post_init__(self):
        self.relationships: Set[Relationship] = set()


    def get_full_name(self, quote_identifier: str) -> str:
        """return the fully quoted schema and name representation of the table."""
        if self.schema is None:
            return f"{quote_identifier}{self.table_name}{quote_identifier}"
        return f"{quote_identifier}{self.schema}{quote_identifier}.{quote_identifier}{self.table_name}{quote_identifier}"


    def to_string(self):
        """String representation of schema.table_name for the table."""
        return f"{self.schema}.{self.table_name}"


    def __eq__(self, other):
        """Overrides the default equality implementation based on object identifiers."""
        if isinstance(other, Table):
            return self.schema == other.schema and self.table_name == other.table_name
        return False


    def __hash__(self) -> int:
        """Overrides the default hash implementation based on object identifiers."""
        return hash(self.schema + "-" + self.table_name)