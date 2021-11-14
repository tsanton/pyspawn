import os
from typing import List
from pytest import fixture
import pyodbc

from pyspawn.Checkpoint import Checkpoint
from pyspawn.graph.Table import Table
from pyspawn.graph.TemporalTable import TemporalTable
from pyspawn.adapters.SqlServerAdapter import SqlServerAdapter


@fixture()
def nuke_sql_server():
    """Server is DNS resolved to service name of MS SQL-container"""
    q1 = "IF EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'SqlServerTests') alter database SqlServerTests set single_user with rollback immediate;"
    q2 = "DROP DATABASE IF EXISTS [SqlServerTests];"
    q3 = "CREATE DATABASE [SqlServerTests];"
    with pyodbc.connect(f"DRIVER=ODBC Driver 17 for SQL Server;SERVER=mssql;DATABASE=tempdb;UID=sa;PWD={os.getenv('SA_PASSWORD')}") as conn:
        conn.autocommit  = True
        with conn.cursor() as cur:
            cur.execute(q1)
            cur.execute(q2)
            cur.execute(q3)

@fixture()
def sql_server_conn(nuke_sql_server):
    """Server is DNS resolved to service name of MS SQL-container"""
    return pyodbc.connect(f"DRIVER=ODBC Driver 17 for SQL Server;SERVER=mssql;DATABASE=SqlServerTests;UID=sa;PWD={os.getenv('SA_PASSWORD')}", autocommit=True)


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



def _create_foreign_key_relationship(sql_server_conn, parent_table: Table, refererenced_table: Table) -> None:
    """The parent_table is the table with the constraint pointing to the refererenced_tables primary key."""
    query = f"""
    ALTER TABLE {parent_table.to_string()}
    ADD CONSTRAINT FK_{parent_table.table_name.upper()}_REFFING_{refererenced_table.table_name} FOREIGN KEY (Val) REFERENCES {refererenced_table.to_string()} (Id)
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
    if is_anonymous_table == True:
        query += "with (system_versioning = on)"
    else:
        query += f"with (system_versioning = on (history_table = {table.history_table_schema}.{table.history_table_name}))"

    _execute_query(sql_server_conn, query)



def test_sql_server_connection(sql_server_conn):
    query = "select @@VERSION Version"
    with sql_server_conn.cursor() as cur:
        res = cur.execute(query).fetchone()[0]
    assert res != None



def test_delete_data(sql_server_conn):
    ### Arrange ###
    A = Table("dbo", "A")
    _create_table(sql_server_conn, A)
    _insert_bulk(sql_server_conn, f"INSERT INTO {A.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    inserted = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {A.to_string()}")
    
    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter())
    Chk.reset(sql_server_conn)

    ### Assert ###
    assert inserted == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {A.to_string()}") == 0, "All records were not deleted"
    sql_server_conn.close()



def test_handle_simple_relationship(sql_server_conn):
    ### Arrange ###
    A = Table("dbo", "A")
    B = Table("dbo", "B")
    _create_table(sql_server_conn, A)
    _create_table(sql_server_conn, B)
    _create_foreign_key_relationship(sql_server_conn, A, B)

    _insert_bulk(sql_server_conn, f"INSERT INTO {A.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    _insert_bulk(sql_server_conn, f"INSERT INTO {B.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {A.to_string()}")
    inserted_b = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {B.to_string()}")
    
    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter())
    Chk.reset(sql_server_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {A.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {B.to_string()}") == 0, "All records were not deleted"
    sql_server_conn.close()



def test_handle_self_relationship(sql_server_conn):
    ### Arrange ###
    A = Table("dbo", "A")
    _create_table(sql_server_conn, A)
    _execute_query(sql_server_conn, f"ALTER TABLE {A.to_string()} ADD CONSTRAINT FK_Parent FOREIGN KEY (Val) references {A.to_string()} (id)")
    _insert_bulk(sql_server_conn, f"INSERT INTO {A.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {A.to_string()}")

    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter())
    Chk.reset(sql_server_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {A.to_string()}") == 0, "All records were not deleted"
    sql_server_conn.close()



def test_handle_simple_circular_relationship(sql_server_conn):
    ### Arrange ###
    A = Table("dbo", "A")
    B = Table("dbo", "B")
    _create_table(sql_server_conn, A)
    _create_table(sql_server_conn, B)
    _create_foreign_key_relationship(sql_server_conn, A, B)
    _create_foreign_key_relationship(sql_server_conn, B, A)

    _insert_bulk(sql_server_conn, f"INSERT INTO {A.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    _insert_bulk(sql_server_conn, f"INSERT INTO {B.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {A.to_string()}")
    inserted_b = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {B.to_string()}")

    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter())
    Chk.reset(sql_server_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {A.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {B.to_string()}") == 0, "All records were not deleted"
    sql_server_conn.close()



def test_handle_complex_cycles(sql_server_conn):
    ### Arrange ###
    A = Table("dbo", "A")
    B = Table("dbo", "B")
    C = Table("dbo", "C")
    D = Table("dbo", "D")
    E = Table("dbo", "E")
    F = Table("dbo", "F")
    _create_table(sql_server_conn, A)
    _execute_query(sql_server_conn, f"CREATE TABLE {B.to_string()} (Id INT PRIMARY Key, a_id INT NULL, c_id INT NULL, d_id INT NULL)")
    _create_table(sql_server_conn, C)
    _create_table(sql_server_conn, D)
    _create_table(sql_server_conn, E)
    _create_table(sql_server_conn, F)
    _create_foreign_key_relationship(sql_server_conn, A, B)
    _execute_query(sql_server_conn, f"alter table {B.to_string()} add constraint FK_b_a foreign key (a_id) references {A.to_string()} (id)")
    _execute_query(sql_server_conn, f"alter table {B.to_string()} add constraint FK_b_c foreign key (c_id) references {C.to_string()} (id)")
    _execute_query(sql_server_conn, f"alter table {B.to_string()} add constraint FK_b_d foreign key (d_id) references {D.to_string()} (id)")
    _create_foreign_key_relationship(sql_server_conn, C, D)
    _create_foreign_key_relationship(sql_server_conn, E, A)
    _create_foreign_key_relationship(sql_server_conn, F, B)

    _execute_query(sql_server_conn, f"INSERT INTO {D.to_string()}(Id) VALUES(1)")
    _execute_query(sql_server_conn, f"INSERT INTO {C.to_string()}(Id, Val) VALUES(1, 1)")
    _execute_query(sql_server_conn, f"INSERT INTO {A.to_string()}(Id) VALUES(1)")
    _execute_query(sql_server_conn, f"INSERT INTO {B.to_string()}(Id, c_id, d_id) VALUES(1, 1, 1)")
    _execute_query(sql_server_conn, f"INSERT INTO {E.to_string()}(Id, Val) VALUES(1, 1)")
    _execute_query(sql_server_conn, f"INSERT INTO {F.to_string()}(Id, Val) VALUES(1, 1)")
    _execute_query(sql_server_conn, f"UPDATE {A.to_string()} set Val = 1")
    _execute_query(sql_server_conn, f"UPDATE {B.to_string()} set a_id = 1")

    i_a = _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {A.to_string()}")
    i_b = _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {B.to_string()}")
    i_c = _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {C.to_string()}")
    i_d = _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {D.to_string()}")
    i_e = _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {E.to_string()}")
    i_f = _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {F.to_string()}")
    
    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter())
    Chk.reset(sql_server_conn)

    ### Assert ###
    assert i_a == 1, "1 record was not inserted to DB"
    assert i_b == 1, "1 record was not inserted to DB"
    assert i_c == 1, "1 record was not inserted to DB"
    assert i_d == 1, "1 record was not inserted to DB"
    assert i_e == 1, "1 record was not inserted to DB"
    assert i_f == 1, "1 record was not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {A.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {B.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {C.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {D.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {E.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {F.to_string()}") == 0, "All records were not deleted"
    sql_server_conn.close()



def test_ignore_tables(sql_server_conn):
    ### Arrange ###
    A = Table("dbo", "A")
    B = Table("dbo", "B")
    _create_table(sql_server_conn, A)
    _create_table(sql_server_conn, B)

    _insert_bulk(sql_server_conn, f"INSERT INTO {A.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    _insert_bulk(sql_server_conn, f"INSERT INTO {B.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {A.to_string()}")
    inserted_b = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {B.to_string()}")
    
    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter(), tables_to_ignore=["A"])
    Chk.reset(sql_server_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {A.to_string()}") == 100, "Records were deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {B.to_string()}") == 0, "All records were not deleted"
    sql_server_conn.close()



def test_include_tables(sql_server_conn):
    ### Arrange ###
    A = Table("dbo", "A")
    B = Table("dbo", "B")
    _create_table(sql_server_conn, A)
    _create_table(sql_server_conn, B)

    _insert_bulk(sql_server_conn, f"INSERT INTO {A.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    _insert_bulk(sql_server_conn, f"INSERT INTO {B.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {A.to_string()}")
    inserted_b = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {B.to_string()}")
    
    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter(), tables_to_include=["A"])
    Chk.reset(sql_server_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {A.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {B.to_string()}") == 100, "Records were deleted"
    sql_server_conn.close()



def test_exclude_schema(sql_server_conn):
    ### Arrange ###
    _create_schema(sql_server_conn, "foo")
    _create_schema(sql_server_conn, "bar")
    A = Table("foo", "A")
    B = Table("bar", "B")
    _create_table(sql_server_conn, A)
    _create_table(sql_server_conn, B)

    _insert_bulk(sql_server_conn, f"INSERT INTO {A.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    _insert_bulk(sql_server_conn, f"INSERT INTO {B.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {A.to_string()}")
    inserted_b = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {B.to_string()}")
    
    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter(), schemas_to_ignore=["foo"])
    Chk.reset(sql_server_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {A.to_string()}") == 100, "Records were deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {B.to_string()}") == 0, "All records were not deleted"
    sql_server_conn.close()



def test_include_schema(sql_server_conn):
    ### Arrange ###
    _create_schema(sql_server_conn, "foo")
    _create_schema(sql_server_conn, "bar")
    A = Table("foo", "A")
    B = Table("bar", "B")
    _create_table(sql_server_conn, A)
    _create_table(sql_server_conn, B)

    _insert_bulk(sql_server_conn, f"INSERT INTO {A.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    _insert_bulk(sql_server_conn, f"INSERT INTO {B.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {A.to_string()}")
    inserted_b = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {B.to_string()}")
    
    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter(), schemas_to_include=["bar"])
    Chk.reset(sql_server_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {A.to_string()}") == 100, "Records were deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {B.to_string()}") == 0, "All records were not deleted"
    sql_server_conn.close()



def test_reseed_id(sql_server_conn):
    ### Arrange ###
    _execute_query(sql_server_conn, f"CREATE TABLE dbo.A (Id INT IDENTITY(1,1), Val INT)")
    _insert_bulk(sql_server_conn, f"INSERT INTO dbo.A (Val) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM dbo.A")

    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter(), reseed_identity=True)
    Chk.reset(sql_server_conn)
    _execute_query(sql_server_conn, f"INSERT INTO dbo.A (Val) values(1234)")

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"Select MAX(Id) from dbo.A") == 1, "Identity did not reset"



def test_reseed_id_two_tables(sql_server_conn):
    ### Arrange ###
    _execute_query(sql_server_conn, f"CREATE TABLE dbo.A (Id INT IDENTITY(1,1), Val INT)")
    _execute_query(sql_server_conn, f"CREATE TABLE dbo.B (Id INT IDENTITY(1,1), Val INT)")
    _insert_bulk(sql_server_conn, f"INSERT INTO dbo.A (Val) values(?)", [[i] for i in range(0, 100)])
    _insert_bulk(sql_server_conn, f"INSERT INTO dbo.B (Val) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM dbo.A")
    inserted_b = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM dbo.B")

    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter(), reseed_identity=True)
    Chk.reset(sql_server_conn)
    _execute_query(sql_server_conn, f"INSERT INTO dbo.A (Val) values(1234)")
    _execute_query(sql_server_conn, f"INSERT INTO dbo.B (Val) values(4321)")

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"Select MAX(Id) from dbo.A") == 1, "Identity did not reset"
    assert _execute_scalar(sql_server_conn, f"Select MAX(Id) from dbo.B") == 1, "Identity did not reset"



def test_reseed_id_table_with_schema(sql_server_conn):
    ### Arrange ###
    _create_schema(sql_server_conn, "base")
    _execute_query(sql_server_conn, f"CREATE TABLE dbo.A (Id INT IDENTITY(1,1), Val INT)")
    _execute_query(sql_server_conn, f"CREATE TABLE base.B (Id INT IDENTITY(1,1), Val INT)")
    _insert_bulk(sql_server_conn, f"INSERT INTO dbo.A (Val) values(?)", [[i] for i in range(0, 100)])
    _insert_bulk(sql_server_conn, f"INSERT INTO base.B (Val) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM dbo.A")
    inserted_b = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM base.B")

    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter(), reseed_identity=True, schemas_to_include=["dbo"])
    Chk.reset(sql_server_conn)
    _execute_query(sql_server_conn, f"INSERT INTO dbo.A (Val) values(1234)")
    _execute_query(sql_server_conn, f"INSERT INTO base.B (Val) values(4321)")

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"Select MAX(Id) from dbo.A") == 1, "Identity did not reset"
    assert _execute_scalar(sql_server_conn, f"Select MAX(Id) from base.B") == 101, "Identity was reseeded"



def test_reseed_id_table_with_table(sql_server_conn):
    ### Arrange ###
    _execute_query(sql_server_conn, f"CREATE TABLE dbo.A (Id INT IDENTITY(1,1), Val INT)")
    _execute_query(sql_server_conn, f"CREATE TABLE dbo.B (Id INT IDENTITY(1,1), Val INT)")
    _insert_bulk(sql_server_conn, f"INSERT INTO dbo.A (Val) values(?)", [[i] for i in range(0, 100)])
    _insert_bulk(sql_server_conn, f"INSERT INTO dbo.B (Val) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM dbo.A")
    inserted_b = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM dbo.B")

    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter(), reseed_identity=True, tables_to_include=["A"])
    Chk.reset(sql_server_conn)
    _execute_query(sql_server_conn, f"INSERT INTO dbo.A (Val) values(1234)")
    _execute_query(sql_server_conn, f"INSERT INTO dbo.B (Val) values(4321)")

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"Select MAX(Id) from dbo.A") == 1, "Identity did not reset"
    assert _execute_scalar(sql_server_conn, f"Select MAX(Id) from dbo.B") == 101, "Identity was reseeded"



def test_reseed_table_never_inserted_data(sql_server_conn):
    ### Arrange ###
    _execute_query(sql_server_conn, f"CREATE TABLE dbo.A (Id INT IDENTITY(1,1), Val INT)")

    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter(), reseed_identity=True)
    Chk.reset(sql_server_conn)
    _execute_query(sql_server_conn, f"INSERT INTO dbo.A (Val) values(0)")

    ### Assert ###
    assert _execute_scalar(sql_server_conn, f"Select MAX(Id) from dbo.A") == 1, "Wrong reseed of Identity in empty table"



def test_reseed_according_to_identity_initial_seed_value(sql_server_conn):
    ### Arrange ###
    _execute_query(sql_server_conn, f"CREATE TABLE dbo.A (Id INT IDENTITY(1001,1), Val INT)")
    _insert_bulk(sql_server_conn, f"INSERT INTO dbo.A (Val) values(?)", [[i] for i in range(0, 100)])
    max_id = _execute_scalar(sql_server_conn,  f"SELECT MAX(id) FROM dbo.A")

    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter(), reseed_identity=True)
    Chk.reset(sql_server_conn)
    _execute_query(sql_server_conn, f"INSERT INTO dbo.A (Val) values(4321)")

    ### Assert ###
    assert max_id == 1100, "Max ID is not as expected"
    assert _execute_scalar(sql_server_conn,  f"SELECT MAX(id) FROM dbo.A") == 1001, "Wrong reseed of Identity compaired to initial seed value"



def test_reseed_according_to_identity_initial_seed_value_table_with_schema(sql_server_conn):
    ### Arrange ###
    _create_schema(sql_server_conn, "base")
    _execute_query(sql_server_conn, f"CREATE TABLE dbo.A (Id INT IDENTITY(1001,1), Val INT)")
    _execute_query(sql_server_conn, f"CREATE TABLE base.B (Id INT IDENTITY(1001,1), Val INT)")
    _insert_bulk(sql_server_conn, f"INSERT INTO dbo.A (Val) values(?)", [[i] for i in range(0, 100)])
    _insert_bulk(sql_server_conn, f"INSERT INTO base.B (Val) values(?)", [[i] for i in range(0, 100)])
    max_id_a = _execute_scalar(sql_server_conn,  f"SELECT MAX(id) FROM dbo.A")
    max_id_b = _execute_scalar(sql_server_conn,  f"SELECT MAX(id) FROM base.B")

    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter(), reseed_identity=True, schemas_to_include=["dbo"])
    Chk.reset(sql_server_conn)
    _execute_query(sql_server_conn, f"INSERT INTO dbo.A (Val) values(1234)")
    _execute_query(sql_server_conn, f"INSERT INTO base.B (Val) values(4321)")

    ### Assert ###
    assert max_id_a == 1100, "Max ID is not as expected"
    assert max_id_b == 1100, "Max ID is not as expected"
    assert _execute_scalar(sql_server_conn, f"Select MAX(id) from dbo.A") == 1001, "Identity did not reset"
    assert _execute_scalar(sql_server_conn, f"Select MAX(Id) from base.B") == 1101, "Identity was reseeded"



def test_reseed_according_to_identity_initial_seed_value_table_with_schema_never_inserted_data(sql_server_conn):
    ### Arrange ###
    _execute_query(sql_server_conn, f"CREATE TABLE dbo.A (Id INT IDENTITY(1001,1), Val INT)")

    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter(), reseed_identity=True)
    Chk.reset(sql_server_conn)
    _execute_query(sql_server_conn, f"INSERT INTO dbo.A (Val) values(0)")

    ### Assert ###
    assert _execute_scalar(sql_server_conn, f"Select MAX(Id) from dbo.A") == 1001, "Wrong reseed of Identity in empty table"



def test_delete_temporal_tables_data(sql_server_conn):
    AT = TemporalTable("dbo", "Foo", "dbo", "FooHistory")
    _create_temporal_table(sql_server_conn, AT)
    
    ### Arrange ###
    _execute_query(sql_server_conn, f"INSERT INTO {AT.schema}.{AT.table_name} (Id) VALUES (1)")
    _execute_query(sql_server_conn, f"UPDATE {AT.schema}.{AT.table_name} SET Id = 2 Where Id = 1")
    _execute_query(sql_server_conn, f"UPDATE {AT.schema}.{AT.table_name} SET Id = 3 Where Id = 2")
    records_at = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {AT.table_to_string()}")
    records_ath = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {AT.history_table_to_string()}")

    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter(), check_temporal_table=True)
    Chk.reset(sql_server_conn)

    ### Assert ###
    assert records_at == 1, "Records in main table not as expected"
    assert records_ath == 2, "Records in history table not as expected"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {AT.table_to_string()}") == 0, "Records were not deleted from temporal table"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {AT.history_table_to_string()}") == 0, "Records were not deleted from temporal history table"



def test_delete_temporal_tables_anonymous_history_table(sql_server_conn):
    AT = TemporalTable("dbo", "Foo", None, None)
    _create_temporal_table(sql_server_conn, AT, is_anonymous_table=True)
    history_table_name = _execute_scalar(sql_server_conn, f"SELECT t1.name FROM sys.tables t1 WHERE t1.object_id = (SELECT history_table_id FROM sys.tables t2 WHERE t2.name = '{AT.table_name}')")
    AT = TemporalTable("dbo", "Foo", "dbo", history_table_name)

    ### Arrange ###
    _execute_query(sql_server_conn, f"INSERT INTO {AT.schema}.{AT.table_name} (Id) VALUES (1)")
    _execute_query(sql_server_conn, f"UPDATE {AT.schema}.{AT.table_name} SET Id = 2 Where Id = 1")
    _execute_query(sql_server_conn, f"UPDATE {AT.schema}.{AT.table_name} SET Id = 3 Where Id = 2")
    records_at = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {AT.table_to_string()}")
    records_ath = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {AT.history_table_to_string()}")

    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter(), check_temporal_table=True)
    Chk.reset(sql_server_conn)

    ### Assert ###
    assert records_at == 1, "Records in main table not as expected"
    assert records_ath == 2, "Records in history table not as expected"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {AT.table_to_string()}") == 0, "Records were not deleted from temporal table"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {AT.history_table_to_string()}") == 0, "Records were not deleted from temporal history table"



def test_delete_temporal_tables_data_different_table_schemas(sql_server_conn):
    _create_schema(sql_server_conn, "base")
    AT = TemporalTable("dbo", "Foo", "base", "FooHistory")
    _create_temporal_table(sql_server_conn, AT)
    
    ### Arrange ###
    _execute_query(sql_server_conn, f"INSERT INTO {AT.schema}.{AT.table_name} (Id) VALUES (1)")
    _execute_query(sql_server_conn, f"UPDATE {AT.schema}.{AT.table_name} SET Id = 2 Where Id = 1")
    _execute_query(sql_server_conn, f"UPDATE {AT.schema}.{AT.table_name} SET Id = 3 Where Id = 2")
    records_at = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {AT.table_to_string()}")
    records_ath = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {AT.history_table_to_string()}")

    ### Act ###
    Chk = Checkpoint(db_adapter=SqlServerAdapter(), check_temporal_table=True)
    Chk.reset(sql_server_conn)

    ### Assert ###
    assert records_at == 1, "Records in main table not as expected"
    assert records_ath == 2, "Records in history table not as expected"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {AT.table_to_string()}") == 0, "Records were not deleted from temporal table"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {AT.history_table_to_string()}") == 0, "Records were not deleted from temporal history table"