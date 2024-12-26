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


# This will return the Average Daily Volume for a specific date
def AverageDailyVolumeNew(n=5, symbol, date=null):
    
    if date:
        
        date_of_query = date(date)
        
        # input t query will be symbol, date of query, limit
        query = f"""
        WITH daily_volume AS (
            SELECT 
                time_bucket(interval '1 day', timestamp) AS bucket,
                SUM(volume) AS daily_volume
            FROM stock_data
            WHERE symbol = %s
            and date(timestamp) = %s 
            GROUP BY bucket
            ORDER BY bucket DESC
            LIMIT %s
        )
        
    else:
    
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
