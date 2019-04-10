-- Database structure checklist.
-- Show databases common view
SELECT pg_database_size($1) -- in bytes
FROM pg_stat_database;