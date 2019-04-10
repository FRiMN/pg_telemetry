import os
from os import environ as env

import psycopg2
from clickhouse_driver import Client
from dotenv import load_dotenv

from collectors import SqlCollector, DtCollector, TsCollector, DBNameCollector, DBPortCollector, DBHostCollector
from sql_files import SqlFiles

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'))


databases = (
    {
        'dbname': env.get('DBNAME'),
        'user': env.get('DBUSER'),
        'host': 'localhost',
        'port': 5432
    },
)


class Store(object):
    client = None

    def __init__(self, host):
        self.client = Client(host=host)
        self._prepare_database()
        self._prepare_rawdata_table()

    def _prepare_database(self):
        sql = "CREATE DATABASE IF NOT EXISTS pg_telemetry"
        return self.client.execute(sql)

    def _prepare_rawdata_table(self):
        sql = """CREATE TABLE IF NOT EXISTS pg_telemetry.raw_data
        (
            dt                  Date,
            ts                  DateTime,
            dbname              String,
            dbhost              String,
            dbport              UInt16,
            dbversion           String,
            settings_hash       String,
            pg_database_size    UInt64
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(dt)
        ORDER BY (ts, dbname, dbhost, dbport)
        """
        return self.client.execute(sql)


if __name__ == '__main__':
    sql_files = SqlFiles(basedir)
    sqls = sql_files.get_sqls()

    store = Store('localhost')

    # print([sql.column_name for sql in sqls])

    for database in databases:
        packet = [
            DtCollector(),
            TsCollector(),
            DBNameCollector(database['dbname']),
            DBPortCollector(database['port']),
            DBHostCollector(database['host']),
            # DBVersionCollector(),
        ]

        conn = psycopg2.connect(**database)
        cur = conn.cursor()

        for sql in sqls:
            collector = SqlCollector(sql, cur, database['dbname'])
            packet.append(collector)

        # print(packet)

        for collector in packet:
            collector.collect()

        print(packet)

        cur.close()
        conn.close()

        store.client.execute("""
        INSERT INTO pg_telemetry.raw_data
        ({}) VALUES
        """.format(','.join([c.column_name for c in packet])),
        [[c.value for c in packet]])
