import psycopg2
import pandas as pd
import boto3
import json
from datetime import datetime, timedelta
import SwingEdgeCore.config.db as DB


conn = DB.connection()

# why are we doing all this? we should simply add the volume of all rows in db for current date
# and return. start/end times do not matter.
# so something like select all candles where to_date(timestamp) == datetime.date.today() 

def TotalCurrentVolume(breakout_till_where_to_calculate="1 hour", symbol="AAPL", 
                       date_for_which_to_calculate="2024-12-23", time_till_which_to_calculate="06:00"):

    global conn

    try:
        cursor = conn.cursor()

      
        end_time = datetime.strptime(f"{date_for_which_to_calculate} {time_till_which_to_calculate}", "%Y-%m-%d %H:%M")
        start_time = end_time - timedelta(hours=17)  

        print(f"[DEBUG] Start Time: {start_time}, End Time: {end_time}")

        
        query = """
            SELECT SUM(volume) AS total_volume
            FROM stock_data
            WHERE symbol = %s
            AND timestamp >= %s
            AND timestamp < %s;
        """

       
        cursor.execute(query, (symbol, start_time, end_time))
        result = cursor.fetchone()
        total_volume = result[0] if result[0] is not None else 0

        print(f"[DEBUG] Total Current Volume for {symbol} from {start_time} to {end_time}: {total_volume}")

       
        cursor.close()

        return total_volume

    except Exception as e:
        print(f"An error occurred while calculating total current volume: {e}")
        return None


