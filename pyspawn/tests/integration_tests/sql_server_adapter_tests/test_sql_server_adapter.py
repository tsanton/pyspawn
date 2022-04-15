from pyspawn import Checkpoint
from pyspawn.adapters import SqlServerAdapter
from pyspawn._graph.table import Table
from pyspawn._graph.temporal_table import TemporalTable

from pyspawn.tests.integration_tests.sql_server_adapter_tests._mssql_utilities import (
    _execute_query,
    _execute_scalar,
    _insert_bulk,
    _create_schema,
    _create_table,
    _create_foreign_key_relationship,
    _create_temporal_table
)


def test_mssql_connection(sql_server_conn):
    query = "select @@VERSION Version"
    with sql_server_conn.cursor() as cur:
        res = cur.execute(query).fetchone()[0]
    assert res is not None


def test_mssql_delete_data(sql_server_conn):
    ### Arrange ###
    a = Table("dbo", "A")
    _create_table(sql_server_conn, a)
    _insert_bulk(sql_server_conn, f"INSERT INTO {a.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    inserted = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {a.to_string()}")
    
    ### Act ###
    checkpoint = Checkpoint(db_adapter=SqlServerAdapter())
    checkpoint.reset(sql_server_conn)

    ### Assert ###
    assert inserted == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {a.to_string()}") == 0, "All records were not deleted"
    sql_server_conn.close()


def test_mssql_handle_simple_relationship(sql_server_conn):
    ### Arrange ###
    a = Table("dbo", "A")
    b = Table("dbo", "B")
    _create_table(sql_server_conn, a)
    _create_table(sql_server_conn, b)
    _create_foreign_key_relationship(sql_server_conn, a, b)

    _insert_bulk(sql_server_conn, f"INSERT INTO {a.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    _insert_bulk(sql_server_conn, f"INSERT INTO {b.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {a.to_string()}")
    inserted_b = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {b.to_string()}")
    
    ### Act ###
    checkpoint = Checkpoint(db_adapter=SqlServerAdapter())
    checkpoint.reset(sql_server_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {a.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {b.to_string()}") == 0, "All records were not deleted"
    sql_server_conn.close()


def test_mssql_handle_self_relationship(sql_server_conn):
    ### Arrange ###
    a = Table("dbo", "A")
    _create_table(sql_server_conn, a)
    _execute_query(sql_server_conn, f"ALTER TABLE {a.to_string()} ADD CONSTRAINT FK_Parent FOREIGN KEY (Val) references {a.to_string()} (id)")
    _insert_bulk(sql_server_conn, f"INSERT INTO {a.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {a.to_string()}")

    ### Act ###
    checkpoint = Checkpoint(db_adapter=SqlServerAdapter())
    checkpoint.reset(sql_server_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {a.to_string()}") == 0, "All records were not deleted"
    sql_server_conn.close()


def test_mssql_handle_simple_circular_relationship(sql_server_conn):
    ### Arrange ###
    a = Table("dbo", "A")
    b = Table("dbo", "B")
    _create_table(sql_server_conn, a)
    _create_table(sql_server_conn, b)
    _create_foreign_key_relationship(sql_server_conn, a, b)
    _create_foreign_key_relationship(sql_server_conn, b, a)

    _insert_bulk(sql_server_conn, f"INSERT INTO {a.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    _insert_bulk(sql_server_conn, f"INSERT INTO {b.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {a.to_string()}")
    inserted_b = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {b.to_string()}")

    ### Act ###
    checkpoint = Checkpoint(db_adapter=SqlServerAdapter())
    checkpoint.reset(sql_server_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {a.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {b.to_string()}") == 0, "All records were not deleted"
    sql_server_conn.close()


def test_mssql_handle_complex_cycles(sql_server_conn):
    ### Arrange ###
    a = Table("dbo", "A")
    b = Table("dbo", "B")
    c = Table("dbo", "C")
    d = Table("dbo", "D")
    e = Table("dbo", "E")
    f = Table("dbo", "F")
    _create_table(sql_server_conn, a)
    _execute_query(sql_server_conn, f"CREATE TABLE {b.to_string()} (Id INT PRIMARY Key, a_id INT NULL, c_id INT NULL, d_id INT NULL)")
    _create_table(sql_server_conn, c)
    _create_table(sql_server_conn, d)
    _create_table(sql_server_conn, e)
    _create_table(sql_server_conn, f)
    _create_foreign_key_relationship(sql_server_conn, a, b)
    _execute_query(sql_server_conn, f"alter table {b.to_string()} add constraint FK_b_a foreign key (a_id) references {a.to_string()} (id)")
    _execute_query(sql_server_conn, f"alter table {b.to_string()} add constraint FK_b_c foreign key (c_id) references {c.to_string()} (id)")
    _execute_query(sql_server_conn, f"alter table {b.to_string()} add constraint FK_b_d foreign key (d_id) references {d.to_string()} (id)")
    _create_foreign_key_relationship(sql_server_conn, c, d)
    _create_foreign_key_relationship(sql_server_conn, e, a)
    _create_foreign_key_relationship(sql_server_conn, f, b)

    _execute_query(sql_server_conn, f"INSERT INTO {d.to_string()}(Id) VALUES(1)")
    _execute_query(sql_server_conn, f"INSERT INTO {c.to_string()}(Id, Val) VALUES(1, 1)")
    _execute_query(sql_server_conn, f"INSERT INTO {a.to_string()}(Id) VALUES(1)")
    _execute_query(sql_server_conn, f"INSERT INTO {b.to_string()}(Id, c_id, d_id) VALUES(1, 1, 1)")
    _execute_query(sql_server_conn, f"INSERT INTO {e.to_string()}(Id, Val) VALUES(1, 1)")
    _execute_query(sql_server_conn, f"INSERT INTO {f.to_string()}(Id, Val) VALUES(1, 1)")
    _execute_query(sql_server_conn, f"UPDATE {a.to_string()} set Val = 1")
    _execute_query(sql_server_conn, f"UPDATE {b.to_string()} set a_id = 1")

    i_a = _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {a.to_string()}")
    i_b = _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {b.to_string()}")
    i_c = _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {c.to_string()}")
    i_d = _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {d.to_string()}")
    i_e = _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {e.to_string()}")
    i_f = _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {f.to_string()}")
    
    ### Act ###
    checkpoint = Checkpoint(db_adapter=SqlServerAdapter())
    checkpoint.reset(sql_server_conn)

    ### Assert ###
    assert i_a == 1, "1 record was not inserted to DB"
    assert i_b == 1, "1 record was not inserted to DB"
    assert i_c == 1, "1 record was not inserted to DB"
    assert i_d == 1, "1 record was not inserted to DB"
    assert i_e == 1, "1 record was not inserted to DB"
    assert i_f == 1, "1 record was not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {a.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {b.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {c.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {d.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {e.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {f.to_string()}") == 0, "All records were not deleted"
    sql_server_conn.close()


def test_mssql_ignore_tables(sql_server_conn):
    ### Arrange ###
    a = Table("dbo", "A")
    b = Table("dbo", "B")
    _create_table(sql_server_conn, a)
    _create_table(sql_server_conn, b)

    _insert_bulk(sql_server_conn, f"INSERT INTO {a.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    _insert_bulk(sql_server_conn, f"INSERT INTO {b.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {a.to_string()}")
    inserted_b = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {b.to_string()}")
    
    ### Act ###
    checkpoint = Checkpoint(db_adapter=SqlServerAdapter(), tables_to_ignore=["A"])
    checkpoint.reset(sql_server_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {a.to_string()}") == 100, "Records were deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {b.to_string()}") == 0, "All records were not deleted"
    sql_server_conn.close()


def test_mssql_include_tables(sql_server_conn):
    ### Arrange ###
    a = Table("dbo", "A")
    b = Table("dbo", "B")
    _create_table(sql_server_conn, a)
    _create_table(sql_server_conn, b)

    _insert_bulk(sql_server_conn, f"INSERT INTO {a.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    _insert_bulk(sql_server_conn, f"INSERT INTO {b.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {a.to_string()}")
    inserted_b = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {b.to_string()}")
    
    ### Act ###
    checkpoint = Checkpoint(db_adapter=SqlServerAdapter(), tables_to_include=["A"])
    checkpoint.reset(sql_server_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {a.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {b.to_string()}") == 100, "Records were deleted"
    sql_server_conn.close()


def test_mssql_exclude_schema(sql_server_conn):
    ### Arrange ###
    _create_schema(sql_server_conn, "foo")
    _create_schema(sql_server_conn, "bar")
    a = Table("foo", "A")
    b = Table("bar", "B")
    _create_table(sql_server_conn, a)
    _create_table(sql_server_conn, b)

    _insert_bulk(sql_server_conn, f"INSERT INTO {a.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    _insert_bulk(sql_server_conn, f"INSERT INTO {b.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {a.to_string()}")
    inserted_b = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {b.to_string()}")
    
    ### Act ###
    checkpoint = Checkpoint(db_adapter=SqlServerAdapter(), schemas_to_ignore=["foo"])
    checkpoint.reset(sql_server_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {a.to_string()}") == 100, "Records were deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {b.to_string()}") == 0, "All records were not deleted"
    sql_server_conn.close()


def test_mssql_include_schema(sql_server_conn):
    ### Arrange ###
    _create_schema(sql_server_conn, "foo")
    _create_schema(sql_server_conn, "bar")
    a = Table("foo", "A")
    b = Table("bar", "B")
    _create_table(sql_server_conn, a)
    _create_table(sql_server_conn, b)

    _insert_bulk(sql_server_conn, f"INSERT INTO {a.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    _insert_bulk(sql_server_conn, f"INSERT INTO {b.to_string()}(Id) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {a.to_string()}")
    inserted_b = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {b.to_string()}")
    
    ### Act ###
    checkpoint = Checkpoint(db_adapter=SqlServerAdapter(), schemas_to_include=["bar"])
    checkpoint.reset(sql_server_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {a.to_string()}") == 100, "Records were deleted"
    assert _execute_scalar(sql_server_conn, f"SELECT COUNT(1) FROM {b.to_string()}") == 0, "All records were not deleted"
    sql_server_conn.close()


def test_mssql_reseed_id(sql_server_conn):
    ### Arrange ###
    a = Table("dbo", "A")
    _execute_query(sql_server_conn, f"CREATE TABLE {a.to_string()} (Id INT IDENTITY(1,1), Val INT)")
    _insert_bulk(sql_server_conn, f"INSERT INTO {a.to_string()} (Val) values(?)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(sql_server_conn,  f"SELECT COUNT(1) FROM {a.to_string()}")

    ### Act ###
    checkpoint = Checkpoint(db_adapter=SqlServerAdapter(), reseed_identity=True)
    checkpoint.reset(sql_server_conn)
    _execute_query(sql_server_conn, f"INSERT INTO {a.to_string()} (Val) values(1234)")

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert _execute_scalar(sql_server_conn, f"Select MAX(Id) from {a.to_string()}") == 1, "Identity did not reset"



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