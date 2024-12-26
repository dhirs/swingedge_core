# import psycopg2
# import pandas as pd
# import boto3
# import json
# from datetime import datetime, timedelta
import SwingEdgeCore.config.db as DB


def TotalCurrentVolume(symbol="AAPL"):

    # global conn

    try:
              
        query = """
            SELECT SUM(volume) AS total_volume
            FROM stock_data
            WHERE symbol = %s
            AND date(timestamp) = current_date;
        """

        return DB.get_one(query,params)
       

    except Exception as e:
        print(f"An error occurred while calculating total current volume: {e}")
        return None
    

def TotalVolumeBetween(symbol, start_time, end_time)

    try:
              
        query = """
            SELECT SUM(volume) AS total_volume
            FROM stock_data
            WHERE timestamp >= %s
            AND timestamp <= %s
            AND date(timestamp) = current_date;
        """

        return DB.get_one(query,params)
       

    except Exception as e:
        print(f"An error occurred while calculating total current volume: {e}")
        return None
