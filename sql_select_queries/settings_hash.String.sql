SELECT md5(
    CAST(array_agg(
        CAST(f.setting as text) order by f.name
    ) as text)
)
FROM pg_settings f
WHERE name != 'application_name';