import os
from os import environ as env

import psycopg2
from dotenv import load_dotenv

from collectors import SqlCollector, DtCollector, TsCollector, DBNameCollector, DBPortCollector, DBHostCollector
from sql_files import SqlFiles
from store import Store

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'))


databases = (
    {
        'dbname': env.get('DBNAME'),
        'user': env.get('DBUSER'),
        'host': env.get('DBHOST'),
        'port': env.get('DBPORT')
    },
)


if __name__ == '__main__':
    sql_files = SqlFiles(basedir)
    sqls = sql_files.get_sqls()

    store = Store('localhost')

    for database in databases:
        collectors = [
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
            collectors.append(collector)

        # print(collectors)

        for collector in collectors:
            collector.collect()

        print(collectors)

        cur.close()
        conn.close()

        store.insert(collectors)
