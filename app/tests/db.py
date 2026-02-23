import sqlite3
import psycopg2


def get_sqlite_connection():
    conn = sqlite3.connect('../data/export_data.sqlite')
    return conn, conn.cursor()


def get_postgres_connection():
    conn = psycopg2.connect(
        dbname='db_dbbe_dev',
        user='db_dbbe_dev',
        password='db_dbbe_dev',
        host='localhost',
        port=15432
    )
    return conn, conn.cursor()