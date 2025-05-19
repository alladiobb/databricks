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
    rides.cpf,
    rides.vehicle_id,
    rides.txn_id,
    current_timestamp() AS load_ts,
    "mongodb" AS source
FROM STREAM(live.raw_vw_py_mongodb_rides) rides
INNER JOIN STREAM(live.hub_users) AS users
    ON rides.cpf = users.cpf
    INNER JOIN STREAM(live.hub_vehicles) AS vehicles
    ON rides.vehicle_id = vehicles.id
    INNER JOIN STREAM(live.hub_payments) AS payments
    ON rides.txn_id = payments.txn_id

