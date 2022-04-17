from typing import List

from pyspawn._graph.table import Table
from pyspawn._graph.temporal_table import TemporalTable


def _execute_query(conn, query):
    with conn.cursor() as cur:
        cur.execute(query)


def _execute_scalar(conn, query):
    with conn.cursor() as cur:
        return cur.execute(query).fetchone()[0]


def _insert_bulk(conn, query, input:List):
    with conn.cursor() as cur:
        cur.executemany(query, input)


def _create_schema(sql_server_conn, schema_name:str) -> None:
    query = f"create schema [{schema_name}]"
    _execute_query(sql_server_conn, query)
    return


def _create_table(sql_server_conn, table: Table) -> None:
    query = f"""
    CREATE TABLE {table.to_string()}
    (
        [Id] int NOT NULL CONSTRAINT PK_{table.table_name} PRIMARY KEY,
        [Val] int 
    )"""
    _execute_query(sql_server_conn, query)


def _create_foreign_key_relationship(sql_server_conn, child_table: Table, parent_table: Table) -> None:
    """The parent_table is the table with the constraint pointing to the refererenced_tables primary key."""
    query = f"""
    ALTER TABLE {child_table.to_string()}
    ADD CONSTRAINT FK_{child_table.table_name.upper()}_REFFING_{parent_table.table_name} FOREIGN KEY (Val) REFERENCES {parent_table.to_string()} (Id)
    """
    _execute_query(sql_server_conn, query)


def _create_temporal_table(sql_server_conn, table: TemporalTable, is_anonymous_table:bool = False) -> None:
    query = f"""
    create table {table.schema}.{table.table_name} 
    (
        [Id] INT NOT NULL PRIMARY KEY CLUSTERED, 
        [SysStartTime] DATETIME2 GENERATED ALWAYS AS ROW START NOT NULL CONSTRAINT CONS_{table.table_name.upper()}_SYSSTART DEFAULT(cast(GETUTCDATE() AS DATETIME2)),
        [SysEndTime] DATETIME2 GENERATED ALWAYS AS ROW END NOT NULL CONSTRAINT CONS_{table.table_name.upper()}_SYSEND DEFAULT(cast('9999-12-31 23:59:99.99' AS DATETIME2)),
        PERIOD FOR SYSTEM_TIME(SysStartTime, SysEndTime)
    )"""
    if is_anonymous_table:
        query += "with (system_versioning = on)"
    else:
        query += f"with (system_versioning = on (history_table = {table.history_table_schema}.{table.history_table_name}))"

    _execute_query(sql_server_conn, query)
