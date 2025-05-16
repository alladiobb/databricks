-- Quando você utiliza DLT (delta files)
-- ingere os arquivos que não foram ingeridos quando utiliza o STREAMING
CREATE OR REFRESH STREAMING TABLE bronze_mssql_users
AS SELECT *, _metadata.file_path AS source_file_path FROM STREAM read_files(
    'dbfs:/mnt/alladio-stg-files/com.alladio.data/mssql/users/',
    format => 'json'
)