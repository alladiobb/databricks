CREATE OR REFRESH STREAMING LIVE TABLE lnk_rides
(
    lnk_rides_id STRING NOT NULL,
    hk_cpf STRING NOT NULL,
    hk_vehicles_id STRING NOT NULL,
    hk_txn_id STRING NOT NULL,
    id STRING NOT NULL,
    cpf STRING NOT NULL,
    vehicle_id LONG NOT NULL,
    txn_id STRING NOT NULL,
    LOAD_TS TIMESTAMP NOT NULL,
    source STRING NOT NULL,
)
AS SELECT
    sha(UPPER(TRIM(CONCAT(users.hk_cpf, vehicles.hk_vehicle_id, payments.hk_txn_id)))) AS lnk_rides_id,
    users.hk_cpf,
    vehicles.hk_vehicle_id,
    payments.hk_txn_id,
    rides.id,
    
