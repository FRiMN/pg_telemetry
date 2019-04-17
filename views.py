class View(object):
    meta_columns = (
        'dt', 'ts', 'dbname', 'dbhost', 'dbport'
    )
    table_name = ''
    sql_template = """
        CREATE VIEW pg_telemetry.{} AS
        {}
        ORDER BY ts DESC
    """
    sql_select = ''

    def __init__(self, client):
        self.client = client

    def _get_exists_tables(self):
        sql = "SHOW TABLES FROM pg_telemetry"
        ret = self.client.execute(sql)
        return [x[0] for x in ret]

    def create(self):
        if self.table_name not in self._get_exists_tables():
            sql_select = self.sql_select.format(','.join(self.meta_columns))
            sql = self.sql_template.format(self.table_name, sql_select)
            print(sql)
            return self.client.execute(sql)


class ResponseTimeView(View):
    table_name = 'response_time'
    sql_select = """
        SELECT {},
            queryid,
            query,
            divide(total_time, calls) AS response_time
        FROM pg_telemetry.pg_stat_statements
    """

class RollbacksView(View):
    table_name = 'rollbacks'
    sql_select = """
        SELECT {},
            runningDifference(xact_rollback)/runningDifference(ts) AS rps
        FROM pg_telemetry.pg_stat_database
    """

class PerfomanceView(View):
    table_name = 'perfomance'
    sql_select = """
        SELECT {},
        ( 
            SELECT
                runningDifference(sum(xact_commit + xact_rollback))/runningDifference(ts) AS tps
            FROM pg_telemetry.pg_stat_database
            GROUP BY ts
        ),
        (
            SELECT
                runningDifference(sum(calls))/runningDifference(ts) AS qps
            FROM pg_telemetry.pg_stat_statements
            GROUP BY ts
        )
        FROM pg_telemetry.pg_stat_statements
        GROUP BY ts
    """ # FIXME: DB::Exception: Scalar subquery returned more than one row.
