data = [[1, "Elia"], [2, "Teo"], [3, "Fang"]]
sdf = spark.createDataFrame(data, schema="id LONG, name STRING")
display(sdf)
