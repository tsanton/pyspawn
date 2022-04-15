import os
from pytest import fixture
from psycopg2._psycopg import connection, cursor
import psycopg2

#Region PgSecrets
PG_HOST = os.getenv("PGSQL_HOST")
PG_DB = os.getenv("PGSQL_DB")
PG_UID = os.getenv("PGSQL_UID")
PG_PWD = os.getenv("PGSQL_PWD")
TESTING_DB = "pg_test"
#EndRegion

@fixture()
def nuke_pg_server():
    """Server is DNS resolved to service name of PG SQL-container"""
    q1 = f"drop database if exists {TESTING_DB};"
    q2 = f"create database {TESTING_DB};"
    conn:connection = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_UID, password=PG_PWD, port=5432)
    conn.autocommit  = True
    cur:cursor = conn.cursor()
    cur.execute(q1)
    cur.execute(q2)
    cur.close()
    conn.close()

@fixture()
def pg_conn(nuke_pg_server) -> connection:
    """Server is DNS resolved to service name of PG SQL-container"""
    conn:connection = psycopg2.connect(host=PG_HOST, dbname=TESTING_DB, user=PG_UID, password=PG_PWD, port=5432)
    conn.autocommit = True
    return conn