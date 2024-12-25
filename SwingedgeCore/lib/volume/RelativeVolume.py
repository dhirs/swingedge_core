import psycopg2
import pandas as pd
import boto3
import json
from datetime import datetime, timedelta
import SwingEdgeCore.config.db as DB

conn = DB.connection()

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


# if __name__ == "__main__":
#     symbol = 'AAPL'
#     n = 2  
#     date = "2024-12-23"
#     time = "06:00"

#     rel_vol = RelativeVolume(symbol=symbol, n=n, breakout="1 hour", date=date, time=time)
#     print(f"Relative Volume for {symbol} as of {date} {time}: {rel_vol}")
