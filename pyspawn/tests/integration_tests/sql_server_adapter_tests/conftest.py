import os
from pytest import fixture
import pyodbc

@fixture()
def nuke_sql_server():
    """Server is DNS resolved to service name of MS SQL-container"""
    q1 = "IF EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'SqlServerTests') alter database SqlServerTests set single_user with rollback immediate;"
    q2 = "DROP DATABASE IF EXISTS [SqlServerTests];"
    q3 = "CREATE DATABASE [SqlServerTests];"
    with pyodbc.connect(f"DRIVER=ODBC Driver 17 for SQL Server;SERVER=mssql;DATABASE=tempdb;UID=sa;PWD={os.getenv('MSSQL_PWD')}") as conn:
        conn.autocommit  = True
        with conn.cursor() as cur:
            cur.execute(q1)
            cur.execute(q2)
            cur.execute(q3)

@fixture()
def sql_server_conn(nuke_sql_server):
    """Server is DNS resolved to service name of MS SQL-container"""
    return pyodbc.connect(f"DRIVER=ODBC Driver 17 for SQL Server;SERVER=mssql;DATABASE=SqlServerTests;UID=sa;PWD={os.getenv('MSSQL_PWD')}", autocommit=True)