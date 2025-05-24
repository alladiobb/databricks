CREATE OR REFRESH STREAMING LIVE TABLE hub_payments
(
  hk_txn_id STRING NOT NULL,
  txn_id LONG NOT NULL,
  load_ts TIMESTAMP,
  source STRING,
  
  CONSTRAINT valid_hk_txn_id EXPECT (hk_txn_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_id EXPECT (txn_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT DISTINCT
  hk_txn_id,
  cpf,
  load_ts,
  "postgres" AS source
FROM STREAM(live.raw_vw_py_mssql_payments);