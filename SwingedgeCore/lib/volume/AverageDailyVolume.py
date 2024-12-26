from DB_config import fetch_single_result
from datetime import datetime, timezone

def get_average_daily_volume(symbol, n):
    try:
        
        current_time = datetime.now(timezone.utc)
        print(f"Current time: {current_time}")

        # Set the cutoff time to 8:00 PM (20:00) UTC
        cutoff_time = current_time.replace(hour=20, minute=0, second=0, microsecond=0)
        print(f"Cutoff time: {cutoff_time}")

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


        if current_time < cutoff_time:
            # If its before 8:00PM exclude today's data
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
            print("Running query excluding today's data...")
            params = (symbol, latest_date, n)
            
        else:
            # If its after 8:00PM include today's data
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
            params = (symbol, latest_date, n)



        result = fetch_single_result(query=query_avg_volume, params=params)


        return round(result, 5) if result is not None else 0

    except Exception as e:
        print("Error calculating average daily volume: ", e)
        return None


if __name__ == "__main__":
    symbol = 'IBM'
    n = 3
    print(f"Result for {symbol}: {get_average_daily_volume(symbol, n)}")


## Lets say an user queries on 26th at 6:00pm for period of 3 (trading time is from 4:00am to 8:00pm).
## Since 6:00 is before 8:00pm we will exclude the 26th and take average of latest three days.
## if the market has closed the current time is after 8:00 pm then take the whole day (curren day) otherwise day - 1
## 4:00am to 8:00pm Eastern Time for the US market -- trading hours