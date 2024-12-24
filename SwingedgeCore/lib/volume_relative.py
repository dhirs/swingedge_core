import psycopg2
import pandas as pd
import boto3
import json
from datetime import datetime, timedelta


ssm = boto3.client('ssm')
parameter = ssm.get_parameter(Name='timescaledb_credentials', WithDecryption=True)
config = json.loads(parameter['Parameter']['Value'])


def get_db_connection():
    try:
        conn = psycopg2.connect(
            database=config['database'],
            user=config['user'],
            password=config['password'],
            host=config['host'],
            port=config['port'],
        )
        print("Connection to the Timescale PostgreSQL established successfully.")
        return conn
    except Exception as e:
        print("Connection to the Timescale PostgreSQL encountered an error: ", e)
        return None

conn = get_db_connection()


def AverageDailyVolume(n, symbol):
    global conn

    try:
        query = f"""
        WITH daily_volume AS (
            SELECT 
                time_bucket(interval '1 day', timestamp) AS bucket,
                SUM(volume) AS daily_volume
            FROM stock_data
            WHERE symbol = %s
            GROUP BY bucket
            ORDER BY bucket DESC
            LIMIT %s
        )
        SELECT 
            ROUND(SUM(daily_volume) / %s, 3) AS average_volume
        FROM daily_volume;
        """

        with conn.cursor() as cur:
            cur.execute(query, (symbol, n, n))
            result = cur.fetchone()
            if result:
                print(f"[DEBUG] Average Daily Volume for {symbol} over last {n} days: {result[0]}")
            else:
                print(f"[DEBUG] No data found for Average Daily Volume calculation.")
        return result[0] if result else None

    except Exception as e:
        print(f"Error while calculating average daily volume for {symbol}: {e}")
        return None


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


def RelativeVolume(symbol, n, breakout="1 hour", date="2024-12-23", time="06:00"):
    try:
        
        total_volume = TotalCurrentVolume(
            breakout_till_where_to_calculate=breakout,
            symbol=symbol,
            date_for_which_to_calculate=date,
            time_till_which_to_calculate=time
        )

        if total_volume is None:
            print(f"Total current volume for {symbol} could not be calculated.")
            return None

        
        avg_daily_volume = AverageDailyVolume(n, symbol)

        if avg_daily_volume is None or avg_daily_volume == 0:
            print(f"Average daily volume for {symbol} over the last {n} days could not be calculated.")
            return None


        relative_volume = round(total_volume / avg_daily_volume, 7)

        print(f"[DEBUG] Relative Volume for {symbol}: {relative_volume} (Total Volume: {total_volume}, Avg Volume: {avg_daily_volume})")

        return relative_volume

    except Exception as e:
        print(f"An error occurred while calculating relative volume: {e}")
        return None


if __name__ == "__main__":
    symbol = 'AAPL'
    n = 2  
    date = "2024-12-23"
    time = "06:00"

    rel_vol = RelativeVolume(symbol=symbol, n=n, breakout="1 hour", date=date, time=time)
    print(f"Relative Volume for {symbol} as of {date} {time}: {rel_vol}")
