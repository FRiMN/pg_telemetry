SELECT xact_rollback
FROM pg_stat_database
WHERE datname = $1;