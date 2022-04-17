from typing import List
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspawn.adapters._db_adapter import DbAdapter
from pyspawn._graph.relationship import Relationship
from pyspawn._graph.temporal_table import TemporalTable
from pyspawn._graph.table import Table
from pyspawn._graph.graph_builder import GraphBuilder


class Checkpoint:
    """Initialize Checkpoint to run reset() between all your integration tests to ensure a clean test DB."""

    def __init__(self, tables_to_ignore: List[str] = [], tables_to_include: List[str] = [], schemas_to_ignore: List[str] = [], schemas_to_include: List[str] = [], check_temporal_table: bool = False, reseed_identity: bool = False, db_adapter:"DbAdapter" = None, command_timeout: int = 120):
        self.tables_to_ignore                         = tables_to_ignore
        self.tables_to_include                        = tables_to_include
        self.schemas_to_ignore                        = schemas_to_ignore
        self.schemas_to_include                       = schemas_to_include
        self.check_temporal_table                     = check_temporal_table
        self.reseed_identity                          = reseed_identity
        self.db_adapter                               = db_adapter
        self.command_timeout                          = command_timeout
        self._database_name: str                      = ""
        self._delete_sql: str                         = ""
        self._reseed_sql: str                         = ""
        self._temporal_tables: List[TemporalTable]    = []
        self._graph_builder: GraphBuilder             = None



    def reset(self, conn):
        """Resets your DB. Expects a connection with autocommit = True"""
        if self._delete_sql == "":
            with conn.cursor() as cur:
                cur.execute(self.db_adapter.get_database_name_command_text())
                self._database_name = cur.fetchone()[0]
            self._build_delete_tables(conn)
        
        if len(self._temporal_tables) > 0:
            turn_off_versioning_cmd_txt = self.db_adapter.build_turn_off_system_versioning_command_text(self._temporal_tables)
            with conn.cursor() as cursor:
                cursor.execute(turn_off_versioning_cmd_txt)

        self._execute_delete_sql(conn)

        if len(self._temporal_tables) > 0:
            turn_on_versioning_cmd_txt = self.db_adapter.build_turn_on_system_versioning_command_text(self._temporal_tables)
            with conn.cursor() as cursor:
                cursor.execute(turn_on_versioning_cmd_txt)


    def _execute_alter_system_versioning(self, conn, cmd_txt: str) -> None:
        """Turn on/off system versioning for temporal tables."""
        with conn.cursor() as cursor:
            cursor.execute(cmd_txt)
        

    def _execute_delete_sql(self, conn) -> None:
        """Execute the batch delete statement, comprised of one or many "delete from", to remove data from tables."""
        with conn.cursor() as cursor:
            cursor.execute(self._delete_sql)
            if self._reseed_sql != "" and self._reseed_sql != None:
                cursor.execute(self._reseed_sql)


    def _build_delete_tables(self, conn) -> None:
        """Main function to create an ordered multiline delete statement that handles system versioned temporal tables and foreign key constraints."""
        all_tables: List[Table] = self._get_all_tables(conn)

        if self.check_temporal_table and self.db_adapter.supports_temporal_tables():
            self._temporal_tables = self._get_all_temporal_tables(conn)
        
        all_relationships = self._get_relationships(conn)

        self._graph_builder = GraphBuilder(all_tables, all_relationships)

        self._delete_sql = self.db_adapter.get_delete_command_text(self._graph_builder)
        self._reseed_sql = self.db_adapter.get_reseed_command_text(self._graph_builder.to_delete) if self.reseed_identity else None



    def _get_relationships(self, conn) -> List[Relationship]:
        """Get relationships consisting of a parent_table (the table with the FK-reference) referencing the referenced_table primary key or unique constrained column."""
        relationships: List[Relationship] = []
        cmd_txt = self.db_adapter.get_relationship_command_text(self)
        with conn.cursor() as cursor:
            cursor.execute(cmd_txt)
            res = cursor.fetchall()
            for i in res:
                relationships.append(Relationship(
                    Table(i[0], i[1]),
                    Table(i[2], i[3]),
                    i[4]))
        return set(relationships)



    def _get_all_tables(self, conn) -> List[Table]:
        """Returns all tables in scope from the checkpoint (include/exclude schemas & tables)."""
        tables: List[Table] = []
        cmd_txt = self.db_adapter.get_tables_command_text(self)
        with conn.cursor() as cursor:
            cursor.execute(cmd_txt)
            res = cursor.fetchall()
            for i in res:
                tables.append(Table(i[0], i[1]))
        return tables


    def _get_all_temporal_tables(self, conn) -> List[TemporalTable]:
        """Returns all temporal tables in scope from the checkpoint (include/exclude schemas & tables)."""
        temporal_tables: List[TemporalTable] = []
        cmd_txt = self.db_adapter.get_temporal_table_command_text(self)
        with conn.cursor() as cursor:
            cursor.execute(cmd_txt)
            res = cursor.fetchall()
            for i in res:
                temporal_tables.append(TemporalTable(i[0], i[1], i[2], i[3]))
        return temporal_tables


    def _does_db_support_temporal_tables(self, conn) -> bool:
        """Check if the db supports temporal tables."""
        if not self.db_adapter.supports_temporal_tables(): 
            return False
        if self._get_sql_server_edition(conn) != 6 and self._get_sql_server_compatability_level(conn) >= 130:
            return True
        return False

 
    def _get_sql_server_edition(self, conn) -> int:
        """Helper function to check if the db supports temporal tables."""
        query = """
        select case
            when serverproperty('EngineEdition') = 5 then 'SQL Azure'
            when serverproperty('EngineEdition') = 6 then 'Azure SQL Data Warehouse'
            when serverproperty('EngineEdition') = 8 then 'Managed Instance'
        end as [edition]
        """
        with conn.cursor() as cursor:
           cursor.execute(query)
           return cursor.fetchone().edition
        return res


    def _get_sql_server_compatability_level(self, conn) -> int:
        """Helper function to check if the db supports temporal tables."""
        query = f"""
        SELECT compatibility_level
        FROM   [sys].[databases]
        WHERE  [name] = '{self._database_name}';
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchone().compatability_level