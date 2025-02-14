from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import to_date, month, year, dayofmonth
from datetime import datetime, timedelta, date
import os
from dotenv import load_dotenv
from SwingedgeCore.db.UsEtfsStocks import UsEtfsStocks

# Load environment vars
load_dotenv()

# Initialize Spark context
sc = SparkContext.getOrCreate()
spark = SparkSession.builder.appName("CDCLoadtoS3").getOrCreate()

def get_credentials():
    params = {
        "db_username": os.getenv("db_username"),
        "db_password": os.getenv("db_password"),
        "db_url": os.getenv("db_url"),
        "db_table": os.getenv("db_table"),
        "s3_bucket": os.getenv("s3_bucket")
    }
    print(params)  
    return params

def get_last_trading_day(reference_date):
    last_trading_day = reference_date - timedelta(days=1)
    while last_trading_day.weekday() in [5, 6]:  # 5 = Saturday, 6 = Sunday
        last_trading_day -= timedelta(days=1)
    return last_trading_day

def get_boundaries(current_date=None):
    try:
        if current_date:
            today_date = current_date
        else:
            today_date = datetime.today()
        
        etf_stock = UsEtfsStocks()

        upper_boundary_date = get_last_trading_day(today_date)
        lower_boundary_date = get_last_trading_day(upper_boundary_date)

        # Query to get max ID before new date insertion
        query_before = """
        SELECT MAX(id) 
        FROM us_etfs_stocks 
        WHERE DATE(timestamp) = %s;
        """
        max_id_before_insertion = etf_stock.execute_query(query_before, params=(lower_boundary_date.date(),), fetch_results=True)

        # Query to get max ID after insertion
        query_after = """
        SELECT MAX(id) 
        FROM us_etfs_stocks 
        """
        max_id_after_insertion = etf_stock.execute_query(query_after, fetch_results=True)

        print(f"Lower Boundary (Max ID from {lower_boundary_date.date()}): {max_id_before_insertion}")
        print(f"Upper Boundary (Max ID from {upper_boundary_date.date()}): {max_id_after_insertion}")

        return max_id_before_insertion, max_id_after_insertion

    except Exception as e:
        print(f"Error: {e}")
        return None, None

def execute_run(current_date=None):
    params = get_credentials()
    if current_date == None:
        current_date = date.today()

    lower_bound, upper_bound = get_boundaries(current_date)

    source_df = spark.read.format("jdbc")\
        .option("user", params["db_username"])\
        .option("password", params["db_password"])\
        .option("url", params["db_url"])\
        .option("dbtable", params["db_table"])\
        .option("numPartitions", 5)\
        .option("partitionColumn", "id")\
        .option("lowerBound", lower_bound)\
        .option("upperBound", upper_bound)\
        .option("driver", "org.postgresql.Driver")\
        .load()

    df1 = source_df.withColumn("date", to_date("timestamp"))
    df2 = df1.withColumn('month', month('date'))
    df3 = df2.withColumn('year', year('date'))
    df4 = df3.withColumn('day', dayofmonth('date'))

    df4.write.mode('append').partitionBy("year", "month", "day").parquet(params["s3_bucket"])

if __name__ == "__main__":
    execute_run()
