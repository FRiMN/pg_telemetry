#!/usr/bin/env python3

"""
Version: 0.4
"""

import os
from os import environ as env

import psycopg2
from clickhouse_driver import Client
from dotenv import load_dotenv

from collectors import PgStatStatementsCollector, PgStatDatabaseCollector
from views import *

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'))


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


if __name__ == '__main__':
    client = Client(**ch_settings)

    for database in databases:
        conn = psycopg2.connect(**database)

        collectors = [
            PgStatStatementsCollector(conn, client),
            PgStatDatabaseCollector(conn, client)
        ]

        for collector in collectors:
            collector.prepare_store()
            collector.save_data_to_store()

        conn.close()

    views = [
        ResponseTimeView(client),
        RollbacksView(client),
        PerfomanceView(client),
        QueryPerfomanceView(client)
    ]
    for view in views:
        view.create()
