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
trip_df.createOrReplaceTempView("trip_df")
zones_df.createOrReplaceTempView("zones_df")

##################Q1############################
start_time = time()
sql1 = spark.sql("SELECT * FROM trip_df where tpep_pickup_datetime LIKE '%-03-%' AND DOLocationID = (SELECT LocationID from zones_df WHERE Zone == 'Battery Park') order by tip_amount desc limit 1")
sql1.collect()
finish_time = time()
sql1.show()
elapsed_time = finish_time - start_time
msg="Time elapsed for q1 is %.4f sec.\n" % (finish_time-start_time)
print(msg)
with open("times-sql_1worker.txt", "a") as outfile:
        outfile.write(msg)
sql1.coalesce(1).write.format("com.databricks.spark.csv").save("hdfs://master:9000/q1_executors1",header='true')
