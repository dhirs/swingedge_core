import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.db import fetch_single_result

def get_total_current_volume(symbol):
    try:
        # Query to fetch the latest timestamp for the given symbol in db
        latest_timestamp_query = """
        SELECT MAX(timestamp) 
        FROM stock_data 
        WHERE symbol = %s;
        """
        latest_timestamp = fetch_single_result(query=latest_timestamp_query, params=(symbol,))
        
        if latest_timestamp is None:
            return 0  

        latest_date = latest_timestamp.date()

        # Query to calculate the total volume for the latest date in db
        total_volume_query = """
        SELECT SUM(volume) 
        FROM stock_data 
        WHERE symbol = %s AND DATE(timestamp) = %s;
        """
        total_volume = fetch_single_result(query=total_volume_query, params=(symbol, latest_date))

        return total_volume if total_volume is not None else 0
    except Exception as e:
        print("Error calculating total current volume: ", e)
        return None

##To do
def TotalVolumeBetween(symbol, start_time, end_time):

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
