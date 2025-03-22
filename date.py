from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder \
    .appName("DE-ETL-102") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
data = [("11/12/2025",), ("27/02.2014",), ("2023.01.09",), ("28-12-2005",)]
df = spark.createDataFrame(data, ["date"])
df_split = df.withColumn("split", split(df["date"], r"[/.-]"))
df_result = df_split.withColumn("day", when(df_split["split"].getItem(0).rlike("^[0-9]{4}$"), df_split["split"].getItem(2)).otherwise(df_split["split"].getItem(0))) \
    .withColumn("month", when(df_split["split"].getItem(0).rlike("^[0-9]{4}$"), df_split["split"].getItem(1)).otherwise(df_split["split"].getItem(1))) \
    .withColumn("year", when(df_split["split"].getItem(0).rlike("^[0-9]{4}$"), df_split["split"].getItem(0)).otherwise(df_split["split"].getItem(2)))
df_result.select("date", "day", "month", "year").show()
