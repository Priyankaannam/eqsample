import sys

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, count

spark = SparkSession\
    .builder\
    .appName("PythonSort")\
    .getOrCreate()

df = spark.read.format("csv").option("header", "true").load("/tmp/data/DataSample.csv")

w = Window.partitionBy(df.columns[1],df.columns[5],df.columns[6])

cleaned_df=(df
    .select(
        "*",
        count("*").over(w).alias("_cnt"))
    .where(col("_cnt") == 1))

cleaned_df.coalesce(1).write.csv('/tmp/data/cleanedfile',header=True)

spark.stop()
