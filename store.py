from clickhouse_driver import Client

from collectors_new import PgStatStatementsCollector


class Store(object):
    """ Класс хранилища метрик. На текущий момент только ClickHouse """
    client = None
    collectors = tuple()

    def __init__(self, client_settings, pg_connection, pg_meta):
        self.client = Client(**client_settings)
        self.pg_connection = pg_connection
        self._set_collectors(pg_meta)
        self._prepare_database()    # TODO: Запускать отдельно по ключу
        # self._prepare_rawdata_table()

    def _set_collectors(self, pg_meta):
        self.collectors = (
            # MetaCollector(pg_meta),
            PgStatStatementsCollector(pg_connection=self.pg_connection, **pg_meta),
            # PgStatDatabaseCollector()
        )

    def _prepare_database(self):
        sql = "CREATE DATABASE IF NOT EXISTS pg_telemetry"
        return self.client.execute(sql)

    # def _prepare_rawdata_table(self):
    #     sql = """CREATE TABLE IF NOT EXISTS pg_telemetry.raw_data
    #     (
    #         dt                  Date,
    #         ts                  DateTime,
    #         dbname              String,
    #         dbhost              String,
    #         dbport              UInt16
    #     ) ENGINE = MergeTree()
    #     PARTITION BY toYYYYMM(dt)
    #     ORDER BY (ts, dbname, dbhost, dbport)
    #     """
    #     return self.client.execute(sql)

    def _get_exists_columns(self, table_name):
        sql = "DESC TABLE pg_telemetry.{}".format(table_name)
        ret = self.client.execute(sql)
        return [x[0] for x in ret]

    def _get_exists_tables(self):
        sql = "SHOW TABLES FROM pg_telemetry"
        ret = self.client.execute(sql)
        return [x[0] for x in ret]

    # def _prepare_rawdata_columns(self, columns):
    #     exist_columns = self._get_exists_columns()
    #     print(exist_columns)
    #
    #     for column, column_type in columns:
    #         if column not in exist_columns:
    #             sql = """ALTER TABLE pg_telemetry.raw_data
    #             ADD COLUMN {} {}""".format(column, column_type)
    #             print(sql)
    #
    #             self.client.execute(sql)

    def _prepare_views(self):
        exist_tables = self._get_exists_tables()
        print(exist_tables)

        sqls = [
            ('response_time', """CREATE VIEW pg_telemetry.response_time AS
                SELECT dt, ts, dbname, dbhost, dbport, 
                divide(pg_stat_statements__sum_total_time, pg_stat_statements__sum_calls) AS response_time
                FROM pg_telemetry.raw_data
                ORDER BY ts DESC
            """),
            ('perfomance', """CREATE VIEW pg_telemetry.perfomance AS
                SELECT dt, ts, dbname, dbhost, dbport, 
                runningDifference(pg_stat_database__xact_commit + pg_stat_database__xact_rollback)/runningDifference(ts) AS tps,
                runningDifference(pg_stat_statements__sum_calls)/runningDifference(ts) AS qps
                FROM pg_telemetry.raw_data
                ORDER BY ts DESC
            """),
            ('rollbacks', """CREATE VIEW pg_telemetry.rollbacks AS
                SELECT dt, ts, dbname, dbhost, dbport, 
                runningDifference(pg_stat_database__xact_rollback)/runningDifference(ts) AS rps
                FROM pg_telemetry.raw_data
                ORDER BY ts DESC
            """)
        ]

        for view_name, sql in sqls:
            if view_name not in exist_tables:
                self.client.execute(sql)

    @staticmethod
    def _convert_values(values):
        converted_values = []
        for value, store_type in values:
            if store_type in ('Int8', 'Int16', 'Int32', 'Int64', 'UInt8', 'UInt16', 'UInt32', 'UInt64'):
                converted_values.append(int(value))
            elif store_type in ('Float32', 'Float64', 'Decimal'):
                converted_values.append(float(value))
            elif store_type == 'String':
                converted_values.append(str(value))
            else:
                converted_values.append(value)
        return converted_values

    def insert(self, collectors):
        columns = [c.column_name for c in collectors]
        columns_with_types = [(c.column_name, c.store_type) for c in collectors]
        values_with_types = [(c.value, c.store_type) for c in collectors]
        values = self._convert_values(values_with_types)

        self._prepare_rawdata_columns(columns_with_types)

        sql = """
            INSERT INTO pg_telemetry.raw_data
            ({}) VALUES
        """.format(','.join(columns))

        ret = self.client.execute(sql, [values])
        self._prepare_views()
        return ret

    # def send_to_store(self):
    #     for collector in self.collectors:
