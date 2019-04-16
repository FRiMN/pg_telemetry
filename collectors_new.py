from datetime import datetime, date

from psycopg2.extras import DictCursor


class BaseCollector(dict):
    _valid_keys = []

    def __init__(self, **kwargs):
        self.pg_connection = kwargs.get('pg_connection')
        self.cursor = self.pg_connection.cursor(cursor_factory=DictCursor)
        super().__init__(**kwargs)

    @property
    def valid_keys(self):
        return self._valid_keys

    def __getitem__(self, key):
        if key not in self.keys():
            if key in self.valid_keys:
                self[key] = getattr(self, key)

        return super().__getitem__(key)


class MetaCollector(BaseCollector):
    __extra_valid_keys = ['ts', 'dt', 'dbhost', 'dbport', 'dbname', 'dbid']
    dbid_sql = """
        SELECT * 
        FROM pg_stat_database 
        WHERE datname = %s
    """

    @property
    def valid_keys(self):
        return super().valid_keys + self.__extra_valid_keys

    @property
    def ts(self):
        return datetime.now()

    @property
    def dt(self):
        return date.today()

    @property
    def dbid(self):
        self.cursor.execute(self.dbid_sql, (self['dbname'],))
        ret = self.cursor.fetchone()
        return ret['datid']


class PgStatStatementsCollector(MetaCollector):
    __extra_valid_keys = (
        'userid',
        # 'dbid',
        'queryid',
        'query',
        'calls',
        'total_time',
        'min_time',
        'max_time',
        'mean_time',
        'stddev_time',
        'rows',
        'shared_blks_hit',
        'shared_blks_read',
        'shared_blks_dirtied',
        'shared_blks_written',
        'local_blks_hit',
        'local_blks_read',
        'local_blks_dirtied',
        'local_blks_written',
        'temp_blks_read',
        'temp_blks_written',
        'blk_read_time',
        'blk_write_time'
    )
    data_sql = """
        SELECT *
        FROM pg_stat_statements
        WHERE dbid = %s;
    """
    data = None

    def get_data(self):
        if not self.data:
            self.cursor.execute(self.data_sql, (self['dbid'],))
            ret = self.cursor.fetchall()
            print(ret[0]['query'])

    # def __getitem__(self, item):

