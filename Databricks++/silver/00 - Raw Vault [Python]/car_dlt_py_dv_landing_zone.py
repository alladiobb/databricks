# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, lit, current_timestamp, expr
@dlt.view()
def raw_vw_by_postgres_vehicle():
    """
    Reads data from the 'bronze_postgres_vehicle' table and applies transformations.

    Args:
        spark: The SparkSession object.

    Returns:
        A DataFrame with selected and transformed columns.
    """
    df = spark.readStream.table("bronze_postgres_vehicle").select(
        sha1(upper(trim(col("id")))).alias("bk_vehicle_id"),
        lit(current_timestamp()).alias("load_ts"),
        col("postgres").alias("source"),
        col("id").alias("id"),
        col("name").alias("name"),
        col("engine").alias("engine"),
        col("max_power").alias("max_power"),
        col("torque").alias("torque"),
        col("mileage").alias("mileage"),
        col("fuel").alias("fuel"),
        col("km_driven").alias("km_driven"),
        col("transmission").alias("transmission"),
        col("seats").alias("seats"),
        col("seller_type").alias("seller_type"),
        col("year").alias("year"),
        expr(
            "CASE "
            "WHEN seats BETWEEN 1 AND 4 THEN 'Compact' "
            "WHEN seats BETWEEN 5 AND 6 THEN 'Standard' "
            "WHEN seats BETWEEN 7 AND 9 THEN 'Multi-Purpose' "
            "WHEN seats BETWEEN 10 AND 14 THEN 'Large Group' "
            "ELSE 'Not-Classified' "
            "END"
        ).alias("drive_type_desc"),
        expr(
            "CASE "
            "WHEN year IN ('2018', '2019', '2020') THEN 'Current' "
            "WHEN year IN ('2013', '2014', '2015', '2016', '2017') THEN 'Recent' "
            "WHEN year IN ('2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012') THEN 'Mid-Age' "
            "WHEN year < '2002' THEN 'Old' "
            "ELSE 'Not-Classified' "
            "END"
        ).alias("car_classification"),
    )
    return df