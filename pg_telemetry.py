import os
from datetime import datetime
from os import environ as env
import os

import psycopg2
from dotenv import load_dotenv


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


sqls = (
    ('pg_database_size', '''
-- Database structure checklist.
-- Show databases common view 
SELECT
  pg_database_size(%s)  -- in bytes
FROM pg_stat_database;
    '''),
)


class Collector(object):
    ts = None
    value = None

    def collect(self):
        raise NotImplementedError


class SqlCollector(Collector):
    def __init__(self, sql, cursor, dbname):
        self.sql = sql
        self.cursor = cursor
        self.dbname = dbname

    def collect(self):
        if self.sql and self.cursor:
            self.cursor.execute(self.sql, (self.dbname,))
            self.ts = datetime.now()
            self.value = self.cursor.fetchone()[0]
            return self.value


class DataPacket(object):
    def __init__(self, value, store_table, ts, dbname, dbport, dbhost, dbsettings, dbversion):
        self.value = value
        self.store_table = store_table
        self.ts = ts
        self.dbname = dbname
        self.dbport = dbport
        self.dbhost = dbhost
        self.dbsettings = dbsettings
        self.dbversion = dbversion


if __name__ == '__main__':
    for database in databases:
        packets = []

        conn = psycopg2.connect(**database)
        cur = conn.cursor()

        database_settings_hash = None

        for store_table, sql in sqls:
            collector = SqlCollector(sql, cur, database['dbname'])
            ret = collector.collect()
            print(ret, collector.sql)

            packet = DataPacket(
                collector.value,
                store_table,
                collector.ts,
                database['dbname'],
                database['port'],
                database['host'],
                database_settings_hash,
                conn.server_version
            )
            packets.append(packet)

        cur.close()
        conn.close()

        print(len(packets))
