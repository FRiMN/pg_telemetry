from datetime import datetime, date


class Collector(object):
    """ Base collector class """
    column_name = None
    value = None

    def collect(self):
        raise NotImplementedError

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

    def collect(self):
        self.value = datetime.now()


class DtCollector(Collector):
    """ Collector return today date """
    column_name = 'dt'

    def collect(self):
        self.value = date.today()


class DumbCollector(Collector):
    def __init__(self, value):
        self.value = value

    def collect(self):
        pass


class DBNameCollector(DumbCollector):
    column_name = 'dbname'


class DBPortCollector(DumbCollector):
    column_name = 'dbport'


class DBHostCollector(DumbCollector):
    column_name = 'dbhost'


class DBVersionCollector(DumbCollector):
    column_name = 'dbversion'


class SqlCollector(Collector):
    def __init__(self, sql, cursor, dbname):
        self.sql = sql.sql
        self.cursor = cursor
        self.dbname = dbname
        self.column_name = sql.column_name

    def collect(self):
        if self.sql and self.cursor:
            sql = self.sql.replace('$1', '%s')
            self.cursor.execute(sql, (self.dbname,))
            self.value = self.cursor.fetchone()[0]
            # return self.value
