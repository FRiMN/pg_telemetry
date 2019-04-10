from datetime import datetime, date


class Collector(object):
    """ Base collector class """
    column_name = None
    value = None

    def collect(self):
        raise NotImplementedError

    def __repr__(self):
        return '<{}: {}={}>'.format(self.__class__.__name__, self.column_name, self.value)


class TsCollector(Collector):
    """ Collector return now datetime """
    column_name = 'ts'

    def collect(self):
        return datetime.now()


class DtCollector(Collector):
    """ Collector return today date """
    column_name = 'dt'

    def collect(self):
        return date.today()


class SqlCollector(Collector):
    def __init__(self, sql, cursor, dbname):
        self.sql = sql.sql
        self.cursor = cursor
        self.dbname = dbname
        self.column_name = sql.column_name

    def collect(self):
        if self.sql and self.cursor:
            self.cursor.execute(self.sql, (self.dbname,))
            self.value = self.cursor.fetchone()[0]
            return self.value
