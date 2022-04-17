import abc
from typing import List
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from pyspawn import Checkpoint
    from pyspawn._graph.graph_builder import GraphBuilder
    from pyspawn._graph.table import Table
    from pyspawn._graph.temporal_table import TemporalTable


class DbAdapter(abc.ABC):

    @property
    def _quote_char(self):
        raise NotImplementedError

    @abc.abstractmethod
    def get_database_name_command_text(self) -> str:
        """Returns a query that yields the database name from the server"""
        pass

    @abc.abstractmethod
    def get_tables_command_text(self, checkpoint: "Checkpoint") -> str:
        """Build a query that selects out all schema- and table names for selected schemas and tables."""
        pass

    @abc.abstractmethod
    def get_temporal_table_command_text(self, checkpoint: "Checkpoint") -> str:
        """Build a query that selects out all temporal table names and schemas with their adjoining historical table- and schema names for selected schemas and tables."""
        pass

    @abc.abstractmethod
    def get_relationship_command_text(self, checkpoint: "Checkpoint") -> str:
        """Build a query that selects out all ForeignKey (FK) to PrimaryKey (PK) relations for selected schemas and tables."""
        pass

    @abc.abstractmethod
    def get_delete_command_text(self, graph: "GraphBuilder") -> str:
        """Build a query that drops cyclical constraints (if any) and deletes tables in an order that does not violate foreign key constraints."""
        pass

    @abc.abstractmethod
    def get_reseed_command_text(self, tables_to_reset: List["Table"]) -> str:
        """Build a query that turns off system versioning for system versioned temporal tables."""
        pass

    @abc.abstractmethod
    def build_turn_off_system_versioning_command_text(self, temporal_tables: List["TemporalTable"]) -> str:
        """Build a query that turns off system versioning for temporal tables."""

    @abc.abstractmethod
    def build_turn_on_system_versioning_command_text(self, temporal_tables: List["TemporalTable"]) -> str:
        """Build a query that turns on system versioning for system versioned temporal tables."""
        pass

    @abc.abstractmethod
    def supports_temporal_tables(self) -> bool:
        """Indicate if the DBAdapter supports temporal tables."""
        pass