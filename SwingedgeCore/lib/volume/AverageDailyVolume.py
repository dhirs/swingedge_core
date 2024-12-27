import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.db import fetch_single_result
from datetime import datetime, timezone

def get_average_daily_volume(*, symbol, n, hour_type='eh'):
    try:
        current_time = datetime.now(timezone.utc)
        current_date = current_time.date()  # Get today's date
        print(f"Current time: {current_time}, Current date: {current_date}")

        # Query to fetch the latest date for the symbol in the database
        query_latest_date = """
            SELECT MAX(DATE(timestamp))
            FROM stock_data
            WHERE symbol = %s;
        """
        latest_date = fetch_single_result(query=query_latest_date, params=(symbol,))
        
        print(f"Latest available date in DB: {latest_date}")
        
        if not latest_date:
            print("No data available for the given symbol.")
            return 0

        # For Extended Hours (4:00 AM to 8:00 PM)
        if hour_type.lower() == 'eh':
            cutoff_time = current_time.replace(hour=20, minute=0, second=0, microsecond=0)
            print(f"Cutoff time: {cutoff_time}")

            if current_time < cutoff_time:
                # If it's before 8:00 PM, exclude today's data
                query_avg_volume = """
                SELECT AVG(daily_volume) 
                FROM (
                    SELECT DATE(timestamp) AS trade_date, SUM(volume) AS daily_volume
                    FROM stock_data
                    WHERE symbol = %s AND DATE(timestamp) < %s
                    GROUP BY DATE(timestamp)
                    ORDER BY trade_date DESC
                    LIMIT %s
                ) AS daily_volumes;
                """
                print("Running query excluding today's data...")
                params = (symbol, current_date, n)
            else:
                # If it's after 8:00 PM, include today's data
                query_avg_volume = """
                SELECT AVG(daily_volume) 
                FROM (
                    SELECT DATE(timestamp) AS trade_date, SUM(volume) AS daily_volume
                    FROM stock_data
                    WHERE symbol = %s AND DATE(timestamp) <= %s
                    GROUP BY DATE(timestamp)
                    ORDER BY trade_date DESC
                    LIMIT %s
                ) AS daily_volumes;
                """
                print("Running query including today's data...")
                params = (symbol, current_date, n)

        # For Market Hours (9:30 AM to 4:00 PM)
        elif hour_type.lower() == 'mh':
            market_end_time = current_time.replace(hour=16, minute=0, second=0, microsecond=0)
            print(f"Market end time: {market_end_time}")

            if current_time < market_end_time:
                # Before 4:00 PM, exclude today's data
                query_avg_volume = """
                SELECT 
                        AVG(daily_volume) AS average_daily_volume
                    FROM (
                        SELECT 
                            DATE(timestamp) AS trade_date, 
                            SUM(volume) AS daily_volume
                        FROM stock_data
                        WHERE symbol = %s
                        AND (
                            (EXTRACT(HOUR FROM timestamp) = 9 AND EXTRACT(MINUTE FROM timestamp) >= 31)
                            OR EXTRACT(HOUR FROM timestamp) BETWEEN 10 AND 15
                        )
                        AND DATE(timestamp) IN (
                            SELECT DISTINCT DATE(timestamp)
                            FROM stock_data
                            WHERE symbol = %s
                            AND DATE(timestamp) < %s
                            ORDER BY DATE(timestamp) DESC
                            LIMIT %s
                        )
                        GROUP BY DATE(timestamp)
                    ) AS daily_volumes
                """
                print("Market hours ongoing; excluding today's data.")
                params = (symbol, symbol, current_date, n)
            else:
                # After 4:00 PM, include today's data
                query_avg_volume = """
                SELECT 
                        AVG(daily_volume) AS average_daily_volume
                    FROM (
                        SELECT 
                            DATE(timestamp) AS trade_date, 
                            SUM(volume) AS daily_volume
                        FROM stock_data
                        WHERE symbol = %s
                        AND (
                            (EXTRACT(HOUR FROM timestamp) = 9 AND EXTRACT(MINUTE FROM timestamp) >= 31)
                            OR EXTRACT(HOUR FROM timestamp) BETWEEN 10 AND 15
                        )
                        AND DATE(timestamp) IN (
                            SELECT DISTINCT DATE(timestamp)
                            FROM stock_data
                            WHERE symbol = %s
                            AND DATE(timestamp) <= %s
                            ORDER BY DATE(timestamp) DESC
                            LIMIT %s
                        )
                        GROUP BY DATE(timestamp)
                    ) AS daily_volumes
                """
                print("Market hours concluded; including today's data.")
                params = (symbol, symbol, current_date, n)
        else:
            print(f"{hour_type} hour type is invalid")
            return 0


        result = fetch_single_result(query=query_avg_volume, params=params)

        return round(result, 5) if result is not None else 0

    except Exception as e:
        print("Error calculating average daily volume: ", e)
        return None

###To Do
def get_avg_volume_specific_dates(symbol,n,date):
    return "volume with specific date"
    

if __name__ == "__main__":
    symbol = 'MSFT'
    n = 3
    print(f"Result for {symbol}: {get_average_daily_volume(symbol=symbol, n=n, hour_type='eh')}")



## Lets say an user queries on 26th at 6:00pm for period of 3 (trading time is from 4:00am to 8:00pm).
## Since 6:00 is before 8:00pm we will exclude the 26th and take average of latest three days.
## if the market has closed the current time is after 8:00 pm then take the whole day (curren day) otherwise day - 1
## 4:00am to 8:00pm Eastern Time for the US market -- trading hours