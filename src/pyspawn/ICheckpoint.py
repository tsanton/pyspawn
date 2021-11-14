from pyspawn.graph.Relationship import Relationship
from pyspawn.graph.TemporalTable import TemporalTable
from pyspawn.graph.Table import Table

import abc
from typing import List
import pyodbc


class ICheckpoint(abc.ABC):
    """Checkpoint interface."""

    @abc.abstractmethod
    def reset(self, conn: pyodbc.Connection):
        """Resets your DB."""
        pass


    @abc.abstractmethod
    def _execute_alter_system_versioning(self, conn: pyodbc.Connection, cmd_txt: str) -> None:
        """Turn on/off system versioning for temporal tables."""
        pass
        

    @abc.abstractmethod
    def _execute_delete_sql(self, conn: pyodbc.Connection, cmd_txt: str) -> None:
        """Execute the batch delete statement, comprised of one or many "delete from", to remove data from tables."""
        pass


    @abc.abstractmethod
    def _build_delete_tables(self, conn: pyodbc.Connection) -> None:
        """Main function to create an ordered multiline delete statement that handles system versioned temporal tables and foreign key constraints."""
        pass


    @abc.abstractmethod
    def _get_relationships(self, conn: pyodbc.Connection) -> List[Relationship]:
        """Get relationships consisting of a parent_table (the table with the FK-reference) referencing the referenced_table primary key or unique constrained column."""
        pass


    @abc.abstractmethod
    def _get_all_tables(self, conn: pyodbc.Connection) -> List[Table]:
        """Returns all tables in scope from the checkpoint (include/exclude schemas & tables)."""
        pass


    @abc.abstractmethod
    def _get_all_temporal_tables(self, conn: pyodbc.Connection) -> List[TemporalTable]:
        """Returns all temporal tables in scope from the checkpoint (include/exclude schemas & tables)."""
        pass


    @abc.abstractmethod
    def _does_db_support_temporal_tables(self, conn: pyodbc.Connection) -> bool:
        """Check if the db supports temporal tables."""
        pass
