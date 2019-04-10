from os import environ as env
import os

import psycopg2
from dotenv import load_dotenv

from collectors import SqlCollector
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


if __name__ == '__main__':
    sql_files = SqlFiles(basedir)
    sqls = sql_files.get_sqls()

    # print([sql.column_name for sql in sqls])

    for database in databases:
        packet = []

        conn = psycopg2.connect(**database)
        cur = conn.cursor()

        for sql in sqls:
            collector = SqlCollector(sql, cur, database['dbname'])
            packet.append(collector)

        cur.close()
        conn.close()

        print(packet)
