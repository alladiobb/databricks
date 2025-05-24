CREATE STREAMING LIVE VIEW raw_vw_mssql_user
AS
SELECT
    sha(UPPER(TRIM(cpf))) AS hk_cpf,
    current_timestamp() AS load_ts,
    "mssql" as source,
    cpf,
    user_id,
    uuid,
    first_name,
    last_name,
    data_birth,
    city,
    country,
    company_name,
FROM STREAM(LIVE.bronze_mssql_users)