import socket
from datetime import datetime, date

from sql_files import Sql


class Collector(object):
    """ Base collector class """
    column_name = None
    _value = None
    store_type = 'String'

    def __init__(self, value):
        self.value = value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    def __repr__(self):
        return '<{}: {}={}{}>'.format(
            self.__class__.__name__,
            self.column_name,
            type(self.value),
            self.value
        )


class TsCollector(Collector):
    """ Collector return now datetime """
    column_name = 'ts'
    store_type = 'DateTime'

    def __init__(self):
        self.value = datetime.now()


class DtCollector(Collector):
    """ Collector return today date """
    column_name = 'dt'
    store_type = 'Date'

    def __init__(self):
        self.value = date.today()


class DBNameCollector(Collector):
    column_name = 'dbname'


class DBPortCollector(Collector):
    column_name = 'dbport'
    store_type = 'UInt16'


class DBHostCollector(Collector):
    column_name = 'dbhost'

    def __init__(self, value):
        super().__init__(value)
        if self.value == 'localhost':
            self.value = socket.gethostbyname(socket.gethostname())


class DBVersionCollector(Collector):
    column_name = 'dbversion'


class SqlCollector(Collector):
    def __init__(self, sql: Sql, cursor, dbname: str):
        self.sql = sql.sql
        self.cursor = cursor
        self.dbname = dbname
        self.column_name = sql.column_name
        if sql.store_type:
            self.store_type = sql.store_type

    def _collect(self):
        if self.sql and self.cursor:
            sql = self.sql.replace('$1', '%s')
            self.cursor.execute(sql, (self.dbname,))
            self.value = self.cursor.fetchone()[0]
            # return self.value

    @Collector.value.getter
    def value(self):
        if self._value is None:
            self._collect()
        return self._value
