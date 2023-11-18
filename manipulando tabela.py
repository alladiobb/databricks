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
