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

##################Q4############################
start_time = time()
q4=trip_df
q4=trip_df.withColumn("Hours",hour("tpep_pickup_datetime")).withColumn("Day of Week",date_format("tpep_pickup_datetime","E"))
q4=q4.groupBy("Day of week","Hours").agg(sum("passenger_count").alias("passengers"))
window = Window.partitionBy("Day of Week").orderBy(desc("passengers"))
q4 = q4.withColumn("rn", row_number().over(window)).filter("rn <= 3")
q4.select("Hours","Day of week","passengers").collect()
finish_time = time()
q4.show()
msg="Time elapsed for q4 is %.4f sec.\n" % (finish_time-start_time)
print(msg)
with open("times-sql_1worker.txt", "a") as outfile:
        outfile.write(msg)
q4.coalesce(1).write.format("com.databricks.spark.csv").save("hdfs://master:9000/q4_executors1",header='true')
