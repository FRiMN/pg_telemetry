import socket
from datetime import datetime, date

from psycopg2.extras import DictCursor


class Collector(object):
    store_tablename = None
    data_sql = None
    extra_column_types = tuple()

    def __init__(self, pg_connection, store_client):
        self.cursor = pg_connection.cursor(cursor_factory=DictCursor)
        self.store_client = store_client

        info = self.cursor.connection.info
        self.dbname = socket.gethostbyname(socket.gethostname()) if info.dbname == 'localhost' else info.dbname
        self.dbhost = info.host
        self.dbport = info.port
        self.dbversion = info.server_version
        self.__dbid = None

    @property
    def column_types(self):
        return (
            ('dt', 'Date'),
            ('ts', 'DateTime'),
            ('dbname', 'String'),
            ('dbhost', 'String'),
            ('dbport', 'UInt16'),
            ('dbsettings_hash', 'String')
        ) + self.extra_column_types

    @property
    def dbid(self):
        if not self.__dbid:
            self.cursor.execute("select datid from pg_stat_database where datname = %s", (self.dbname,))
            self.__dbid = self.cursor.fetchone()['datid']

        return self.__dbid

    @property
    def dbsettings_hash(self):
        sql = """
        SELECT md5(
            CAST(array_agg(
                CAST(f.setting as text) order by f.name
            ) as text)
        ) as hash
        FROM pg_settings f
        WHERE name != 'application_name';
        """
        self.cursor.execute(sql)
        return self.cursor.fetchone()['hash']

    def _get_exists_tables(self):
        sql = "SHOW TABLES FROM pg_telemetry"
        ret = self.store_client.execute(sql)
        return [x[0] for x in ret]

    def prepare_store(self):
        exist_tables = self._get_exists_tables()
        column_defenition = ['{} {}'.format(x[0], x[1]) for x in self.column_types]
        if self.store_tablename not in exist_tables:
            sql = """CREATE TABLE pg_telemetry.{}
                 (
                     {}
                 ) ENGINE = MergeTree()
                 PARTITION BY toYYYYMM(dt)
                 ORDER BY (ts, dbname, dbhost, dbport)
                 """.format(self.store_tablename, ','.join(column_defenition))
            return self.store_client.execute(sql)

    def get_data(self):
        self.cursor.execute(self.data_sql, (self.dbid,))
        data = self.cursor.fetchall()
        return data

    def clean_data(self, data):
        cleaned_data = []
        meta_data = {
            'ts'    : datetime.now(),
            'dt'    : date.today(),
            'dbname': self.dbname,
            'dbhost': self.dbhost,
            'dbport': self.dbport,
            'dbsettings_hash': self.dbsettings_hash
        }
        for d in data:
            rd = dict(**d)
            rd.update(meta_data)
            cleaned_data.append(rd)
        return cleaned_data

    def save_data_to_store(self):
        data = self.clean_data(self.get_data())
        columns = [x[0] for x in self.column_types]
        sql = """
                    INSERT INTO pg_telemetry.{}
                    ({}) VALUES
                """.format(self.store_tablename, ','.join(columns))

        return self.store_client.execute(sql, data)


class PgStatStatementsCollector(Collector):
    store_tablename = 'pg_stat_statements'
    extra_column_types = (
        ('userid', 'UInt32'),
        # 'dbid',
        ('queryid', 'UInt32'),
        ('query', 'String'),
        ('calls', 'UInt64'),
        ('total_time', 'Float32'),
        ('min_time', 'Float32'),
        ('max_time', 'Float32'),
        ('mean_time', 'Float32'),
        ('stddev_time', 'Float32'),
        ('rows', 'UInt64'),
        ('shared_blks_hit', 'UInt32'),
        ('shared_blks_read', 'UInt32'),
        ('shared_blks_dirtied', 'UInt32'),
        ('shared_blks_written', 'UInt32'),
        ('local_blks_hit', 'UInt32'),
        ('local_blks_read', 'UInt32'),
        ('local_blks_dirtied', 'UInt32'),
        ('local_blks_written', 'UInt32'),
        ('temp_blks_read', 'UInt32'),
        ('temp_blks_written', 'UInt32'),
        ('blk_read_time', 'Float32'),
        ('blk_write_time', 'Float32')
    )

    data_sql = """
        SELECT *
        FROM pg_stat_statements
        WHERE dbid = %s;
    """

    def clean_data(self, data):
        data = super().clean_data(data)

        for d in data:
            if d['queryid'] is None:
                d['queryid'] = 0

        return data


class PgStatDatabaseCollector(Collector):
    store_tablename = 'pg_stat_database'
    extra_column_types = (
        # ('datid', ''),
        # ('datname', ''),
        ('numbackends', 'UInt8'),
        ('xact_commit', 'UInt32'),
        ('xact_rollback', 'UInt32'),
        ('blks_read', 'UInt64'),
        ('blks_hit', 'UInt64'),
        ('tup_returned', 'UInt64'),
        ('tup_fetched', 'UInt64'),
        ('tup_inserted', 'UInt64'),
        ('tup_updated', 'UInt64'),
        ('tup_deleted', 'UInt64'),
        ('conflicts', 'UInt32'),
        ('temp_files', 'UInt32'),
        ('temp_bytes', 'UInt64'),
        ('deadlocks', 'UInt32'),
        ('blk_read_time', 'Float32'),
        ('blk_write_time', 'Float32'),
        ('stats_reset', 'DateTime')
    )

    data_sql = """
        select *
        from pg_stat_database
        where datid = %s;
    """
