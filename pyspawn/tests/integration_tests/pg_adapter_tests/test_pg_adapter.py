from pyspawn import Checkpoint
from pyspawn.adapters import PgAdapter
from pyspawn._graph.table import Table

from pyspawn.tests.integration_tests.pg_adapter_tests._pgsql_utilities import (
    _execute_query,
    _execute_scalar,
    _insert_bulk,
    _create_schema,
    _create_table,
    _create_foreign_key_relationship
)

def test_pg_connection(pg_conn):
    query = "select version()"
    with pg_conn.cursor() as cur:
        cur.execute(query)
        res:str = cur.fetchone()[0]
    assert res is not None


def test_pg_delete_data(pg_conn):
    ### Arrange ###
    a = Table("public", "a")
    _create_table(pg_conn, a)
    _insert_bulk(pg_conn, f"INSERT INTO {a.to_string()}(Id) values(%s)", [[i] for i in range(0, 100)])
    inserted = _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {a.to_string()}")

    ### Act ###
    checkpoint = Checkpoint(db_adapter=PgAdapter())
    checkpoint.reset(pg_conn)

    ### Assert ###
    assert inserted == 100, "100 records were not inserted to DB"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {a.to_string()}") == 0, "All records were not deleted"
    pg_conn.close()


def test_pg_handle_simple_relationship(pg_conn):
    ### Arrange ###
    a = Table("public", "a")
    b = Table("public", "b")
    _create_table(pg_conn, a)
    _create_table(pg_conn, b)
    _create_foreign_key_relationship(pg_conn, a, b)

    _insert_bulk(pg_conn, f"INSERT INTO {a.to_string()}(id) values(%s)", [[i] for i in range(0, 100)])
    _insert_bulk(pg_conn, f"INSERT INTO {b.to_string()}(id) values(%s)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {a.to_string()}")
    inserted_b = _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {b.to_string()}")

    ### Act ###
    checkpoint = Checkpoint(db_adapter=PgAdapter())
    checkpoint.reset(pg_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {a.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {b.to_string()}") == 0, "All records were not deleted"
    pg_conn.close()


def test_pg_handle_self_relationship(pg_conn):
    ### Arrange ###
    a = Table("public", "a")
    _create_table(pg_conn, a)
    _execute_query(pg_conn, f"alter table {a.to_string()} add constraint fk_parent foreign key (val) references {a.to_string()} (id)")
    _insert_bulk(pg_conn, f"INSERT INTO {a.to_string()}(Id) values(%s)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(pg_conn,  f"SELECT COUNT(1) FROM {a.to_string()}")

    ### Act ###
    checkpoint = Checkpoint(db_adapter=PgAdapter())
    checkpoint.reset(pg_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {a.to_string()}") == 0, "All records were not deleted"
    pg_conn.close()


def test_pg_handle_simple_circular_relationship(pg_conn):
    ### Arrange ###
    a = Table("public", "a")
    b = Table("public", "b")
    _create_table(pg_conn, a)
    _create_table(pg_conn, b)
    _create_foreign_key_relationship(pg_conn, a, b)
    _create_foreign_key_relationship(pg_conn, b, a)

    _insert_bulk(pg_conn, f"INSERT INTO {a.to_string()}(Id) values(%s)", [[i] for i in range(0, 100)])
    _insert_bulk(pg_conn, f"INSERT INTO {b.to_string()}(Id) values(%s)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(pg_conn,  f"SELECT COUNT(1) FROM {a.to_string()}")
    inserted_b = _execute_scalar(pg_conn,  f"SELECT COUNT(1) FROM {b.to_string()}")

    ### Act ###
    checkpoint = Checkpoint(db_adapter=PgAdapter())
    checkpoint.reset(pg_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {a.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {b.to_string()}") == 0, "All records were not deleted"
    pg_conn.close()


def test_pg_handle_complex_cycles(pg_conn):
    ### Arrange ###
    a = Table("public", "a")
    b = Table("public", "b")
    c = Table("public", "c")
    d = Table("public", "d")
    e = Table("public", "e")
    f = Table("public", "f")
    _create_table(pg_conn, a)
    _execute_query(pg_conn, f"create table {b.to_string()} (id int primary key, a_id int null, c_id int null, d_id int null)")
    _create_table(pg_conn, c)
    _create_table(pg_conn, d)
    _create_table(pg_conn, e)
    _create_table(pg_conn, f)
    _create_foreign_key_relationship(pg_conn, a, b)
    _execute_query(pg_conn, f"alter table {b.to_string()} add constraint fk_b_a foreign key (a_id) references {a.to_string()} (id)")
    _execute_query(pg_conn, f"alter table {b.to_string()} add constraint fk_b_c foreign key (c_id) references {c.to_string()} (id)")
    _execute_query(pg_conn, f"alter table {b.to_string()} add constraint fk_b_d foreign key (d_id) references {d.to_string()} (id)")
    _create_foreign_key_relationship(pg_conn, c, d)
    _create_foreign_key_relationship(pg_conn, e, a)
    _create_foreign_key_relationship(pg_conn, f, b)

    _execute_query(pg_conn, f"insert into {d.to_string()}(id) values(1)")
    _execute_query(pg_conn, f"insert into {c.to_string()}(id, val) values(1, 1)")
    _execute_query(pg_conn, f"insert into {a.to_string()}(id) values(1)")
    _execute_query(pg_conn, f"insert into {b.to_string()}(id, c_id, d_id) values(1, 1, 1)")
    _execute_query(pg_conn, f"insert into {e.to_string()}(id, val) values(1, 1)")
    _execute_query(pg_conn, f"insert into {f.to_string()}(id, val) values(1, 1)")
    _execute_query(pg_conn, f"update {a.to_string()} set val = 1")
    _execute_query(pg_conn, f"update {b.to_string()} set a_id = 1")

    i_a = _execute_scalar(pg_conn, f"select count(1) from {a.to_string()}")
    i_b = _execute_scalar(pg_conn, f"select count(1) from {b.to_string()}")
    i_c = _execute_scalar(pg_conn, f"select count(1) from {c.to_string()}")
    i_d = _execute_scalar(pg_conn, f"select count(1) from {d.to_string()}")
    i_e = _execute_scalar(pg_conn, f"select count(1) from {e.to_string()}")
    i_f = _execute_scalar(pg_conn, f"select count(1) from {f.to_string()}")

    ### Act ###
    checkpoint = Checkpoint(db_adapter=PgAdapter())
    checkpoint.reset(pg_conn)

    ### Assert ###
    assert i_a == 1, "1 record was not inserted to DB"
    assert i_b == 1, "1 record was not inserted to DB"
    assert i_c == 1, "1 record was not inserted to DB"
    assert i_d == 1, "1 record was not inserted to DB"
    assert i_e == 1, "1 record was not inserted to DB"
    assert i_f == 1, "1 record was not inserted to DB"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {a.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {b.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {c.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {d.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {e.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {f.to_string()}") == 0, "All records were not deleted"
    pg_conn.close()


def test_pg_ignore_tables(pg_conn):
    ### Arrange ###
    a = Table("public", "a")
    b = Table("public", "b")
    _create_table(pg_conn, a)
    _create_table(pg_conn, b)

    _insert_bulk(pg_conn, f"INSERT INTO {a.to_string()}(id) values(%s)", [[i] for i in range(0, 100)])
    _insert_bulk(pg_conn, f"INSERT INTO {b.to_string()}(id) values(%s)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {a.to_string()}")
    inserted_b = _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {b.to_string()}")

    ### Act ###
    checkpoint = Checkpoint(db_adapter=PgAdapter(), tables_to_ignore=["a"])
    checkpoint.reset(pg_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {a.to_string()}") == 100, "Records were deleted"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {b.to_string()}") == 0, "All records were not deleted"
    pg_conn.close()


def test_pg_include_tables(pg_conn):
    ### Arrange ###
    a = Table("public", "a")
    b = Table("public", "b")
    _create_table(pg_conn, a)
    _create_table(pg_conn, b)

    _insert_bulk(pg_conn, f"INSERT INTO {a.to_string()}(Id) values(%s)", [[i] for i in range(0, 100)])
    _insert_bulk(pg_conn, f"INSERT INTO {b.to_string()}(Id) values(%s)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {a.to_string()}")
    inserted_b = _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {b.to_string()}")

    ### Act ###
    checkpoint = Checkpoint(db_adapter=PgAdapter(), tables_to_include=["a"])
    checkpoint.reset(pg_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {a.to_string()}") == 0, "All records were not deleted"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {b.to_string()}") == 100, "Records were deleted"
    pg_conn.close()


def test_pg_exclude_schema(pg_conn):
    ### Arrange ###
    _create_schema(pg_conn, "foo")
    _create_schema(pg_conn, "bar")
    a = Table("foo", "a")
    b = Table("bar", "b")
    _create_table(pg_conn, a)
    _create_table(pg_conn, b)

    _insert_bulk(pg_conn, f"INSERT INTO {a.to_string()}(Id) values(%s)", [[i] for i in range(0, 100)])
    _insert_bulk(pg_conn, f"INSERT INTO {b.to_string()}(Id) values(%s)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {a.to_string()}")
    inserted_b = _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {b.to_string()}")

    ### Act ###
    checkpoint = Checkpoint(db_adapter=PgAdapter(), schemas_to_ignore=["foo"])
    checkpoint.reset(pg_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {a.to_string()}") == 100, "Records were deleted"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {b.to_string()}") == 0, "All records were not deleted"
    pg_conn.close()


def test_pg_include_schema(pg_conn):
    ### Arrange ###
    _create_schema(pg_conn, "foo")
    _create_schema(pg_conn, "bar")
    a = Table("foo", "a")
    b = Table("bar", "b")
    _create_table(pg_conn, a)
    _create_table(pg_conn, b)

    _insert_bulk(pg_conn, f"INSERT INTO {a.to_string()}(Id) values(%s)", [[i] for i in range(0, 100)])
    _insert_bulk(pg_conn, f"INSERT INTO {b.to_string()}(Id) values(%s)", [[i] for i in range(0, 100)])
    inserted_a = _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {a.to_string()}")
    inserted_b = _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {b.to_string()}")

    ### Act ###
    checkpoint = Checkpoint(db_adapter=PgAdapter(), schemas_to_include=["bar"])
    checkpoint.reset(pg_conn)

    ### Assert ###
    assert inserted_a == 100, "100 records were not inserted to DB"
    assert inserted_b == 100, "100 records were not inserted to DB"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {a.to_string()}") == 100, "Records were deleted"
    assert _execute_scalar(pg_conn, f"SELECT COUNT(1) FROM {b.to_string()}") == 0, "All records were not deleted"
    pg_conn.close()


# def test_pg_reseed_identity(pg_conn):
#     ### Arrange ###
#     a = Table("public", "a")
#     _execute_query(pg_conn, f"CREATE TABLE {a.to_string()} (id INT GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1), Val INT)")
#     _insert_bulk(pg_conn, f"INSERT INTO {a.to_string()} (Val) values(%s)", [[i] for i in range(0, 100)])
#     inserted_a = _execute_scalar(pg_conn,  f"SELECT COUNT(1) FROM {a.to_string()}")
#
#     ### Act ###
#     checkpoint = Checkpoint(db_adapter=PgAdapter(), reseed_identity=True)
#     checkpoint.reset(pg_conn)
#     _execute_query(pg_conn, f"INSERT INTO {a.to_string()} (Val) values(1234)")
#
#     ### Assert ###
#     assert inserted_a == 100, "100 records were not inserted to DB"
#     assert _execute_scalar(pg_conn, f"Select MAX(Id) from {a.to_string()}") == 1, "Identity did not reset"


# def test_pg_reseed_serial(sql_server_conn):
#     pass