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
            sql_select = self.sql_select.format(mc=','.join(self.meta_columns))
            sql = self.sql_template.format(self.table_name, sql_select)
            print(sql)
            return self.client.execute(sql)


class ResponseTimeView(View):
    table_name = 'response_time'
    sql_select = """
        SELECT {mc},
            queryid,
            query,
            divide(total_time, calls) AS response_time
        FROM pg_telemetry.pg_stat_statements
    """


class RollbacksView(View):
    table_name = 'rollbacks'
    sql_select = """
        SELECT {mc},
            runningDifference(xact_rollback)/runningDifference(ts) AS rps
        FROM pg_telemetry.pg_stat_database
    """


class PerformanceView(View):
    table_name = 'performance'
    sql_select = """
        SELECT {mc},
            tps,
            qps
        FROM (
                SELECT {mc}, 
                    runningDifference(sum(calls)) / runningDifference(ts) AS qps
                FROM pg_telemetry.pg_stat_statements
                GROUP BY {mc}
             )
        ANY FULL JOIN (
                SELECT {mc}, 
                    runningDifference(xact_commit + xact_rollback) / runningDifference(ts) AS tps
                FROM pg_telemetry.pg_stat_database
            )
        USING {mc}
    """


class QueryPerfomanceView(View):
    table_name = 'query_perfomance'
    sql_select = """
        SELECT {mc}, 
            substringUTF8(
                replaceRegexpAll(query, '[\n\t ]+', ' '), 1, 100
            ) AS query_sample,
            queryid,
            anyHeavy(userid),
            runningDifference(sum(calls)) / runningDifference(ts) AS qps
        FROM pg_telemetry.pg_stat_statements
        GROUP BY {mc}, query, queryid
    """ # FIXME: Работает не правильно, runningDifference ходит по разным query


class CacheHitRatioView(View):
    table_name = 'cache_hit_ratio'
    sql_select = """
        SELECT {mc},
            -- show cache hit ratio, values closer to 100 are better
            round(100 * sum(blks_hit) / sum(blks_hit + blks_read), 3) as cache_hit_ratio
        FROM pg_telemetry.pg_stat_database
        GROUP BY {mc}
    """

class FetchedRowsRatio(View):
    table_name = 'fetched_rows_ratio'
    sql_select = """
        SELECT {mc},
            -- show fetched rows ratio, values closer to 100 are better
            round(100 * sum(tup_fetched) / sum(tup_fetched + tup_returned), 3) as fetched_ratio
        FROM pg_telemetry.pg_stat_database
        GROUP BY {mc}
    """
