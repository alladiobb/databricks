#Schema define
from pyspark.sql.types import StructType, IntegerType, DateType,
StringType, DecimalType, FloatType
Record_schema = (StructType().
add("registro", StringType()).
add("pais", StringType()).
add("descricao", StringType()).
add("destino",StringType()).
add("pontos",FloatType()).
add("preco",FloatType()).
add("provincia",StringType()).
add("regiao_1",StringType()).
add("regiao_2",StringType()).
add("somelier",StringType()).
add("twiter_somelier",StringType()).
add("endereco",StringType()).
add("variante",StringType()).
add("vinicola",StringType())
)

#Load table
# File location and type
file_location = "/FileStore/tables/carga/vinhos_no_mundo.csv"
file_type = "csv"
# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","
# The applied options are for CSV files. For other file types,
these will be ignored.
df = spark.read.format(file_type).schema(Record_schema) \
.option("inferSchema", infer_schema) \
.option("header", first_row_is_header) \
.option("sep", delimiter) \
.load(file_location)
display(df)

#Create Temp VIEW
# Create a view or table
temp_table_name = "vinhos_no_mundo_csv"
df.createOrReplaceTempView(temp_table_name)


#Play consulting temporary view
%sql
/* Query the created temp table in a SQL cell */
select * from `vinhos_no_mundo_csv`
