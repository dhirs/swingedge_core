# Script to load all historical candle data into AWS S3.

import sys
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark import SparkConf
from pyspark.sql import SparkSession

# These are not needed by the job but only for job.commit to work
from awsglue.context import GlueContext
from awsglue.job import Job
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
job = Job(glueContext)
# -------------------------
spark = SparkSession.builder.appName("DefaultSparkSession").getOrCreate()

# parameters
db_username = 'tsdbadmin'
db_password = 'letin1234$Agf'
db_url = 'jdbc:postgresql://w25uxmcny6.m8iecjpjs8.tsdb.cloud.timescale.com:30127/tsdb'
table_name = 'us_etfs_stocks'
s3_bucket = 's3://alphavantage-us-stocks/data'

# numpartitions is 80. this will initiate 80  tasks as we had a lot of data
# for base load 
# lowerbound, uppperbound is the lowest and highest number for the id column
# system will get rows between these numbers and divide into numpartitions     
source_df = spark.read.format("jdbc")\
            .option("url",db_url)\
            .option("user", db_username)\
            .option("password", db_password)\
            .option("dbtable",table_name)\
            .option("numPartitions", 80)\
            .option("partitionColumn", "id")\
            .option("lowerBound", 1395571)\
            .option("upperBound", 97585572)\
            .option("driver","org.postgresql.Driver")\
            .load()

df1 = source_df.withColumn("date",to_date("timestamp"))
df2 = df1.withColumn('month', month('date'))
df3 = df2.withColumn('year', year('date'))
df4 = df3.withColumn('day', dayofmonth('date'))
# print(df4.rdd.getNumPartitions())

# Append will simply add the files to bucket
# Overwrite will erase previous data and then add the files
df4.write.mode('append').partitionBy("year","month","day").parquet(s3_bucket)
# df4.write.mode('overwrite').partitionBy("month").parquet(s3_bucket)


# This statement is only needed so we can commit the job and get the spark UI to show proper details. It isnot needed by the core logic
job.commit()