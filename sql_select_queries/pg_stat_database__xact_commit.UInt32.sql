SELECT xact_commit
FROM pg_stat_database
WHERE datname = $1;