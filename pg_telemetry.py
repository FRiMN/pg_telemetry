#!/usr/bin/env python3

"""
Version: 0.5
"""
import os
from os import environ as env
from threading import Timer

import psycopg2
from clickhouse_driver import Client
from dotenv import load_dotenv

from collectors import PgStatStatementsCollector, PgStatDatabaseCollector, DatabaseSizeCollector
from views import *

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'))

fetch_interval = float(env.get('FETCH_INTERVAL', 60))


databases = (
    {
        'dbname': env.get('DBNAME'),
        'user': env.get('DBUSER'),
        'host': env.get('DBHOST'),
        'port': env.get('DBPORT'),
        'password': env.get('DBPASSWORD')
    },
)

ch_settings = {
    'host'       : env.get('CHHOST', 'localhost'),
    'port'       : env.get('CHPORT', 9000),
    'password'   : env.get('CHPASSWORD', ''),
    'user'       : env.get('CHUSER', 'default'),
    'client_name': 'pg_telemetry',
    'compression': True
}

client = Client(**ch_settings)


STORE_VIEWS_CREATED = False
def make_store_views():
    global STORE_VIEWS_CREATED
    if not STORE_VIEWS_CREATED:
        print('creating view')
        views = [
            ResponseTimeView(client),
            RollbacksView(client),
            PerformanceView(client),
            # QueryPerfomanceView(client)
            CacheHitRatioView(client),
            FetchedRowsRatioView(client),
        ]
        for view in views:
            view.drop()
            view.create()
        STORE_VIEWS_CREATED = True


def fetch_data(database):
    conn = psycopg2.connect(**database)

    collectors = [
        PgStatStatementsCollector(conn, client),
        PgStatDatabaseCollector(conn, client),
        DatabaseSizeCollector(conn, client)
    ]

    for collector in collectors:
        collector.prepare_store()
        collector.save_data_to_store()

    conn.close()


def timed_task(database):
    t = Timer(fetch_interval, timed_task, [database])
    t.start()
    fetch_data(database)
    make_store_views()


if __name__ == '__main__':
    for database in databases:
        t = Timer(0, timed_task, [database])
        t.start()
