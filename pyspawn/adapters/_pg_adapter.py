from typing import List
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pyspawn._graph.graph_builder import GraphBuilder
    from pyspawn._graph.temporal_table import TemporalTable
    from pyspawn._graph.table import Table
    from pyspawn import Checkpoint
from pyspawn.adapters._db_adapter import DbAdapter

class PgAdapter(DbAdapter):
    _quote_char = '"'


    def __init__(self):
        super().__init__()


    def get_database_name_command_text(self) -> str:
        return "SELECT current_database()"


    def get_tables_command_text(self, checkpoint: "Checkpoint") -> str:
        """Build a query that selects out all schema- and table names for scoped schemas and tables."""
        cmd_txt:str = """
                select
                       table_schema,
                       table_name
                from information_schema.tables
                where table_type = 'BASE TABLE'
                and table_schema not in('pg_catalog', 'information_schema')
                """
        if len(checkpoint.tables_to_ignore) > 0:
            tables_to_ignore = ",".join(["'" + x + "'" for x in checkpoint.tables_to_ignore])
            cmd_txt += f"and table_name not in ({tables_to_ignore})\n"

        if len(checkpoint.tables_to_include) > 0:
            tables_to_include = ",".join(["'" + x + "'" for x in checkpoint.tables_to_include])
            cmd_txt += f"and table_name in ({tables_to_include})\n"

        if len(checkpoint.schemas_to_ignore) > 0:
            schemas_to_ignore = ",".join(["'" + x + "'" for x in checkpoint.schemas_to_ignore])
            cmd_txt += f"and table_schema not in ({schemas_to_ignore})\n"

        if len(checkpoint.schemas_to_include) > 0:
            schemas_to_include = ",".join(["'" + x + "'" for x in checkpoint.schemas_to_include])
            cmd_txt += f"and table_schema in ({schemas_to_include})\n"
        return cmd_txt


    def get_temporal_table_command_text(self, checkpoint: "Checkpoint") -> str:
        raise NotImplementedError("Temporal tables are not supported for Postgres")


    def get_relationship_command_text(self, checkpoint: "Checkpoint") -> str:
        """Build a query that selects out all ForeignKey (FK) to PrimaryKey (PK) relations for scoped schemas and tables."""
        cmd_txt:str = """
        select 
            tc.table_schema child_schema_name,  
            tc.table_name child_table_name, 
            ctu.table_schema parent_schema_name, 
            ctu.table_name parent_table_name, 
            rc.constraint_name foreign_key_name
        from information_schema.referential_constraints rc
        inner join information_schema.constraint_table_usage ctu ON ctu.constraint_name = rc.constraint_name
        inner join information_schema.table_constraints tc ON tc.constraint_name = rc.constraint_name
        where 1=1
        """
        if len(checkpoint.tables_to_ignore) > 0:
            tables_to_ignore = ",".join(["'" + x + "'" for x in checkpoint.tables_to_ignore])
            cmd_txt += f"and ctu.table_name not in ({tables_to_ignore})\n"

        if len(checkpoint.tables_to_include) > 0:
            tables_to_include = ",".join(["'" + x + "'" for x in checkpoint.tables_to_include])
            cmd_txt += f"and ctu.table_name in ({tables_to_include})\n"

        if len(checkpoint.schemas_to_ignore) > 0:
            schemas_to_ignore = ",".join(["'" + x + "'" for x in checkpoint.schemas_to_ignore])
            cmd_txt += f"and ctu.table_schema not in ({schemas_to_ignore})\n"

        if len(checkpoint.schemas_to_include) > 0:
            schemas_to_include = ",".join(["'" + x + "'" for x in checkpoint.schemas_to_include])
            cmd_txt += f"and ctu.table_schema in ({schemas_to_include})\n"
        return cmd_txt


    def get_delete_command_text(self, graph: "GraphBuilder") -> str:
        """Build a query that drops cyclical constraints (if any) and deletes tables in an order that does not violate foreign key constraints."""
        cmd_txt = ""

        for r in graph.cyclic_relationships:
            cmd_txt += f"ALTER TABLE {r.parent_table.get_full_name(self._quote_char)} DISABLE TRIGGER ALL;\n"

        if len(graph.to_delete) > 0:
            all_tables:List[str] = ",".join([t.get_full_name(self._quote_char) for t in graph.to_delete])
            cmd_txt += f"truncate table {all_tables} cascade\n;"

        for r in graph.cyclic_relationships:
            cmd_txt += f"ALTER TABLE {r.parent_table.get_full_name(self._quote_char)} ENABLE TRIGGER ALL;\n"

        return cmd_txt


    def get_reseed_command_text(self, tables_to_reset: List["Table"]) -> str:
        """
        Postgres has two sequence types, the "identity" column *type* and the serial *pseudo-type*.
        In the case of serial, some behind-the-scenes work is done to create a column of type int or
        bigint and then place a generated sequence behind it. Since both can be reseeded we can check
        for both, within the constraints of the tables the user wants to reset.
        This more complex implementation accommodates use cases where the user may have renamed their
        sequences in a manner where a regex parse would fail.
        """
        table_names:List[str] = ",".join(["'" + x.get_full_name(self._quote_char) + "'" for x in tables_to_reset])
        cmd_txt:str = f"""
        CREATE OR REPLACE FUNCTION pg_temp.reset_sequence(seq text, start_val int, increment_val int) RETURNS void AS $$
        DECLARE
        BEGIN
            /* ALTER SEQUENCE doesn't work with variables, so we construct a statement and execute that instead. */
            EXECUTE 'alter sequence ' || seq || ' restart with ' || start_val || ' increment ' || increment_val;
        END;
        $$ LANGUAGE plpgsql;
        
        WITH all_sequences AS 
        (
            SELECT 
                pg_get_serial_sequence(quote_ident(table_schema) || '.' || quote_ident(table_name), column_name) AS sequence_name,
                is_identity,
                --coalesce to handle sequences whom do not have start or increment values (fixed 1,1)
                coalesce(identity_start, '1')::int start_val,
                coalesce(identity_increment, '1')::int increment_val
            FROM information_schema.columns
            WHERE pg_get_serial_sequence(quote_ident(table_schema) || '.' || quote_ident(table_name), column_name) IS NOT NULL
            AND  '"' || table_schema || '"."' || table_name || '"' IN ({table_names})
        )
        SELECT pg_temp.reset_sequence(s.sequence_name, s.start_val, s.increment_val) FROM all_sequences s;
        """
        return cmd_txt


    def build_turn_off_system_versioning_command_text(self, temporal_tables: List["TemporalTable"]) -> str:
        raise NotImplementedError("Temporal tables are not supported for Postgres")


    def build_turn_on_system_versioning_command_text(self, temporal_tables: List["TemporalTable"]) -> str:
        raise NotImplementedError("Temporal tables are not supported for Postgres")


    def supports_temporal_tables(self) -> bool:
        return False



