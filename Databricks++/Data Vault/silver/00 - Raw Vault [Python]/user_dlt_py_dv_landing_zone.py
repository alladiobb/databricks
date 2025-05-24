@dlt.view()
def raw_vw_py_mssql_user():
    """
    This function reads data from the raw vault landing zone and returns a DataFrame.
    The data is filtered to include only records where the 'is_active' column is True.
    """
    return (
        dlt.readStream("bronze_mssql_user").select(
            sha(upper(trim(col("cpf")))).alias("hk_cpf"),
            lit(current_timestamp()).alias("load_ts"),
            lit("mssql").alias("source"),
            col("user_id").alias("user_id"),
            col("uuid").alias("uuid"),
            col("cpf").alias("cpf"),
            concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"),
            col("date_birth").cast(DateType()).alias("date_birth"),
            col("phone_number").alias("phone_number"),
            col("compnay_name").alias("company_name"),
            col("job").alias("job"),
            col("city").alias("city"),
            col("country").alias("country")
        )
    )