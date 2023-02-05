from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from time import time
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
print("spark session created")

df1 = spark.read.option("header", "true").option("inferSchema", "true").format("parquet").parquet("hdfs://master:9000/myProject/yellow_tripdata_2022-01.parquet", "hdfs://master:9000/myProject/yellow_tripdata_2022-02.parquet", "hdfs://master:9000/myProject/yellow_tripdata_2022-03.parquet", "hdfs://master:9000/myProject/yellow_tripdata_2022-04.parquet","hdfs://master:9000/myProject/yellow_tripdata_2022-05.parquet","hdfs://master:9000/myProject/yellow_tripdata_2022-06.parquet")
zones_df = spark.read.option("header", "true").option("inferSchema", "true").format("csv").csv("hdfs://master:9000/myProject/taxi+_zone_lookup.csv")

#######remove dates outside January-June 2022#############################
df1 = df1.filter(df1.tpep_pickup_datetime > "2022-01-01").filter(df1.tpep_pickup_datetime < "2022-06-30")


####################DROPS ROWS WITH NULL VALUES####################
trip_df = df1.na.drop("any")

############################NAME THE TABLE###########################
#trip_df.createOrReplaceTempView("trip_df")
#zones_df.createOrReplaceTempView("zones_df")

##################sort by date###########################################
#trip_df.sort("tpep_pickup_datetime").show(truncate=False)
##################Q1############################
trip_rdd = trip_df.rdd

grouped_rdd = trip_rdd.map(lambda x: ((x['tpep_pickup_datetime'].strftime('%Y-%m-%d'), (x['PULocationID'], x['DOLocationID'])), [x['trip_distance'], x['fare_amount']])) \
  .reduceByKey(lambda a, b: a + b) \
  .map(lambda x: (x[0][0], [x[1][0] / len(x[1]), x[1][1] / len(x[1])]))

result_rdd = grouped_rdd.map(lambda x: (x[0], x[1][0], x[1][1])) \
  .sortByKey() \
  .map(lambda x: (x[0], x[1], x[2]))

result_rdd.collect().foreach(println)
