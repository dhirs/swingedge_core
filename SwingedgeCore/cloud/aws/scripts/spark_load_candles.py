from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import to_date
from pyspark.sql import SparkSession
from datetime import date
from dotenv import load_dotenv
import os

# load environment vars
load_dotenv()

# initialize spark context
sc = SparkContext.getOrCreate()
spark = SparkSession.builder.appName("CDCLoadtoS3").getOrCreate()

# get credentials from SSM
def get_credentials():
    # get these from SSM
    db_username = 'tsdbadmin'
    db_password = 'letin1234$Agf'
    db_url = 'jdbc:postgresql://w25uxmcny6.m8iecjpjs8.tsdb.cloud.timescale.com:30127/tsdb'
    db_table = 'us_etfs_stocks'
    s3_bucket = 's3://alphavantage-us-stocks/data'
    return db_username,db_password,db_url,db_table,s3_bucket

# get boundaries for incremental update
def get_boundaries(current_date):
    lower_bound = ''
    upper_bound = ''
    return lower_bound, upper_bound
    

# main executor
def execute_run(current_date=None):
    
    db_username,db_password, db_url, db_table,s3_bucket = get_credentials()
    if current_date == None:
        current_date = date.today()
        
    lower_bound, upper_bound = get_boundaries(current_date)
    
    source_df = spark.read.format("jdbc")\
            .option("user", db_username)\
            .option("password", db_password)\
            .option("url",db_url)\
            .option("dbtable",db_table)\
            .option("numPartitions", 5)\
            .option("partitionColumn", "id")\
            .option("lowerBound", lower_bound)\
            .option("upperBound", upper_bound)\
            .option("driver","org.postgresql.Driver")\
            .load()

    df1 = source_df.withColumn("date",to_date("timestamp"))
    df2 = df1.withColumn('month', month('date'))
    df3 = df2.withColumn('year', year('date'))
    df4 = df3.withColumn('day', dayofmonth('date'))
    
    df4.write.mode('append').partitionBy("year","month","day").parquet(s3_bucket)
    
    
if __name__ == "__main__":  
    date_to_run = os.getenv("date_to_run")
    execute_run(date_to_run)
    