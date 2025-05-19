CREATE OR REFRESH STREAMING LIVE TABLE hub_vehicles
(
  hk_vehicles_id STRING NOT NULL,
  id LONG NOT NULL,
  load_ts TIMESTAMP,
  source STRING,
  
  CONSTRAINT valid_hk_vehicle_id EXPECT (hk_vehicle_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT DISTINCT
  hk_vehicles_id,
  cpf,
  load_ts,
  "mssql" AS source
FROM STREAM(live.raw_vw_py_mssql_vehicle);