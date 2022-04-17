from typing import List

from pyspawn._graph.table import Table

def _execute_query(pg_conn, query) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(query)


def _execute_scalar(pg_conn, query):
    with pg_conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone()[0]


def _insert_bulk(pg_conn, query, input:List):
    with pg_conn.cursor() as cur:
        cur.executemany(query, input)


def _create_schema(conn, schema_name:str) -> None:
    query = f"create schema {schema_name}"
    _execute_query(conn, query)
    return


def _create_table(pg_conn, table: Table) -> None:
    query = f"""
    CREATE TABLE {table.to_string()}
    (
        id int NOT NULL CONSTRAINT pk_{table.table_name} PRIMARY KEY,
        val int 
    )"""
    _execute_query(pg_conn, query)


def _create_foreign_key_relationship(pg_conn, child_table: Table, parent_table: Table) -> None:
    """The parent_table is the table with the constraint pointing to the referenced_tables primary key."""
    query = f"""
    alter table {child_table.to_string()}
    add constraint fk_{child_table.table_name.upper()}_reffing_{parent_table.table_name} foreign key (val) references {parent_table.to_string()} (id)
    """
    _execute_query(pg_conn, query)