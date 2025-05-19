CREATE OR REFRESH STREAMING LIVE TABLE sat_mongodb_rides
(
    lnk_rides_id STRING NOT NULL,
    id STRING,
    user_id LONG,
    vehicle_id LONG,
    txn_id STRING,
    subscription_id LONG,
    product_id STRING,
    loc_source STRING,
    loc_destination STRING,
    distance_miles DOUBLE,
    distance_km DOUBLE,
    name STRING,
    cab_type STRING,
    price_usd DOUBLE,
    price_brl DOUBLE,
    dynamic_fare DOUBLE,
    dynamic_fare_brl DOUBLE,
    time_stamp LONG,
    load_ts TIMESTAMP,
    source STRING

    CONSTRAINT valid_lnk_rides_id EXPECT (lnk_rides_id IS NOT NULL) ON VIOLATION DROP ROW,
)
AS SELECT
    lnk_rides.lnk_rides_id,
    lnk_rides.id,

    rw_rides.id,
    rw_rides.vehicle_id,
    rw_rides.cpf,
    rw_rides.txn_id,
    current_timestamp() AS load_ts,
    "mongodb" AS source
FROM STREAM(live.raw_vw_py_mongodb_rides) rides
INNER JOIN STREAM(live.hub_users) AS users
    ON rides.cpf = users.cpf
    INNER JOIN STREAM(live.hub_vehicles) AS vehicles
    ON rides.vehicle_id = vehicles.id
    INNER JOIN STREAM(live.hub_payments) AS payments
    ON rides.txn_id = payments.txn_id

