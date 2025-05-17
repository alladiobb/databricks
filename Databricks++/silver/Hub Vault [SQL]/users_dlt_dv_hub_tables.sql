CREATE OR REFRESH STREAMING LIVE TABLE hub_users
(
  hk_cpf STRING NOT NULL,
  cpf STRING NOT NULL,
  load_ts TIMESTAMP,
  source STRING,
  
  CONSTRAINT valid_hk_cpf EXPECT (hk_cpf IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_cpf EXPECT (cpf IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT DISTINCT
  hk_cpf,
  cpf,
  load_ts,
  "mssql" AS source
FROM STREAM(live.raw_vw_py_mssql_user)
UNION ALL
SELECT DISTINCT
  hk_cpf,
  cpf,
  load_ts,
  "mongodb" AS source
FROM STREAM(live.raw_vw_py_mongodb_user);