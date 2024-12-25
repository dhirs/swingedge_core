import psycopg2
import pandas as pd
import boto3
import json
from datetime import datetime, timedelta
import SwingEdgeCore.config.db as DB


def AverageDailyVolume(n=5, symbol):
   
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
    
    response = DB.get_one(query,(symbol,n))
    return response



