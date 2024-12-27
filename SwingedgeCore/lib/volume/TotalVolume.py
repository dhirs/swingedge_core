import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.db import fetch_single_result


def get_total_volume(*, symbol, hour_type='eh'):
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

        ##Extended hours
        if hour_type.lower() == 'eh':
            # For Extended Hours: get total volume for the latest date
            total_volume_query = """
            SELECT SUM(volume) 
            FROM stock_data 
            WHERE symbol = %s AND DATE(timestamp) = %s;
            """
            params = (symbol, latest_date)
        
        ##Market hours
        elif hour_type.lower() == 'mh':
            # For Market Hours: get total volume from 9:31 AM to 3:59 PM
            total_volume_query = """
            SELECT SUM(volume) 
            FROM stock_data 
            WHERE symbol = %s 
            AND DATE(timestamp) = %s 
            AND (
                (EXTRACT(HOUR FROM timestamp) = 9 AND EXTRACT(MINUTE FROM timestamp) >= 31)
                OR EXTRACT(HOUR FROM timestamp) BETWEEN 10 AND 15
            );
            """
            params = (symbol, latest_date)

        else:
            print(f"{hour_type} hour type is invalid")
            return 0

        # Execute the query to calculate the total volume
        total_volume = fetch_single_result(query=total_volume_query, params=params)

        return total_volume if total_volume is not None else 0

    except Exception as e:
        print("Error calculating total current volume: ", e)
        return None

##To Do
def get_total_volume_specific_dates(symbol,n,date):
    return "total volume with specific date"
    
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


if __name__ == "__main__":
    print("Result: ", get_total_volume(symbol='MSFT', hour_type='mh'))


##matplotlib candle chart data --> IBM for hourly candle (part of lib library)