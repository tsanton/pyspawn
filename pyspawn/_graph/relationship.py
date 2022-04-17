from pyspawn._graph.table import Table


class Relationship:
    """A relationship consists of a parent_table (the table with the FK-reference) referencing the referenced_table primary key or unique constrained column."""

    def __init__(self, parent_table: Table, referenced_table: Table, relationship_name: str) -> None:
        self.parent_table = parent_table
        self.referenced_table = referenced_table
        self.relationship_name = relationship_name


    def to_string(self):
        """String representation of relationship where parent_table is the table with the FK-reference referencing the referenced_table primary key or unique constrained column."""
        return f"{self.parent_table} -> {self.referenced_table} [{self.relationship_name}]"


    def __eq__(self, other):
        """Overrides the default equality implementation based on object identifiers."""
        if isinstance(other, Relationship):
            return self.relationship_name == other.relationship_name
        return False


    def __hash__(self) -> int:
        """Overrides the default hash implementation based on object identifiers."""
        return hash(self.relationship_name)