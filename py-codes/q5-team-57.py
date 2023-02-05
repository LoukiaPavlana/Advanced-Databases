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

##################Q5############################
start_time = time()
q5=trip_df
q5=trip_df.withColumn("Month",month("tpep_pickup_datetime")).withColumn("Day",dayofmonth("tpep_pickup_datetime")).withColumn("tip percentage",(trip_df["tip_amount"]/trip_df["fare_amount"]))
q5=q5.groupBy("Month","Day").agg(avg("tip percentage").alias("avg tip percentage per day"))
window = Window.partitionBy("Month").orderBy(desc("avg tip percentage per day"))
q5 = q5.withColumn("rn", row_number().over(window)).filter("rn <= 5")
q5.select("Month","Day","avg tip percentage per day").collect()
finish_time = time()
q5.show()
elapsed_time = finish_time - start_time
msg="Time elapsed for q5 is %.4f sec.\n" % (finish_time-start_time)
print(msg)
with open("times-sql_1worker.txt", "a") as outfile:
        outfile.write(msg)
q5.coalesce(1).write.format("com.databricks.spark.csv").save("hdfs://master:9000/q5_executors1",header='true')
