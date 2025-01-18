##Import package
##use this when package is not ready
# import sys
# import os
# Add the root directory to the Python path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


import psycopg2
from psycopg2.extras import execute_values
from ..cloud.aws.ssm import Credentials   ##use this when package is done
# from SwingedgeCore.cloud.aws.ssm import Credentials

# Database Connection Handler
class DatabaseConnection:
    def __init__(self, db_name=None):
        self.db_name = db_name

    def __get_timescaledb_connection(self):
        cred = Credentials(credential_name='timescale')
        db_config = cred.get_credentials()
        try:
            conn = psycopg2.connect(
                database=db_config['database'],
                user=db_config['user'],
                password=db_config['password'],
                host=db_config['host'],
                port=db_config['port'],
            )
            print("Connection to the Timescale PostgreSQL established successfully.")
            return conn
        except Exception as e:
            print("Connection to the Timescale PostgreSQL encountered an error: ", e)
            return None

    def get_connection(self):
        if self.db_name and self.db_name.lower() == 'timescaledb':
            return self.__get_timescaledb_connection()
        else:
            raise ValueError("Unsupported database specified or missing database name.")

# Entity-specific Upload Handler
class TimescaleDBUploader:
    def __init__(self, connection, entity):
        self.connection = connection
        self.entity = entity
        if not self.connection:
            raise ValueError("Database connection is not established.")
        self.table_name = self.__resolve_entity()

    def __resolve_entity(self):
        if self.entity.lower() == 'getcandledata':
            # return "stock_dummy"
            return "us_etfs_stocks"
        else:
            raise ValueError(f"Entity '{self.entity}' is not recognized.")

    def upload_to_timescaledb(self, data_to_upload, metadata=None):
        if not data_to_upload:
            print(f"No data to upload for {metadata}.")
            return False

        try:
            columns = list(data_to_upload[0].keys())
            column_names = ", ".join(columns)
            query_values = [tuple(record.values()) for record in data_to_upload]

            query_template = f"""
            INSERT INTO {self.table_name} ({column_names})
            VALUES %s
            ON CONFLICT (symbol, timestamp)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
            """

            with self.connection.cursor() as cur:
                execute_values(cur, query_template, query_values)
                self.connection.commit()

            print(f"Data successfully upserted in table '{self.table_name}' for symbol={metadata}.")
            return True
        except Exception as e:
            print(f"Error while preparing or executing the query: {e}")
            return False

# General Query Executor
class QueryExecutor:
    def __init__(self, connection):
        self.connection = connection
        if not self.connection:
            raise ValueError("Database connection is not established.")

    def execute_query(self, query, params=None):
        """Executes a single query with optional parameters."""
        try:
            with self.connection.cursor() as cur:
                cur.execute(query, params)
                if query.strip().lower().startswith("select"):
                    result = cur.fetchall()
                    return result
                self.connection.commit()
            print("Query executed successfully.")
            return True
        except Exception as e:
            print(f"Error executing query: {e}")
            return False

    def execute_many(self, query, data):
        """Executes a query for multiple rows of data."""
        try:
            with self.connection.cursor() as cur:
                execute_values(cur, query, data)
                self.connection.commit()
            print("Batch query executed successfully.")
            return True
        except Exception as e:
            print(f"Error executing batch query: {e}")
            return False


class DBTimestamps:
    def __init__(self, db_connection=None):
        if db_connection:
            self.query_executor = QueryExecutor(db_connection)  
        else:
            self.query_executor = self._get_default_query_executor()
        
        if not self.query_executor:
            raise ValueError("QueryExecutor instance is required.")
        
    def _get_default_query_executor(self):
        default_connection = DatabaseConnection(db_name="timescaledb").get_connection()  
        return QueryExecutor(default_connection)

    def fetch_recent_timestamps(self):
        try:
            query = """
                SELECT symbol, MAX(timestamp) as latest_timestamp
                FROM us_etfs_stocks
                GROUP BY symbol
                ORDER BY latest_timestamp DESC
            """
            raw_results = self.query_executor.execute_query(query)
            if not raw_results:
                print("No results found or query execution failed.")
                return []

            formatted_results = [
                {'symbol': row[0], 'timestamp': row[1].strftime('%Y-%m-%d %H:%M:%S')}
                for row in raw_results
            ]

            print(f"Recent timestamps fetched successfully. Total: {len(formatted_results)}")
            return formatted_results

        except Exception as e:
            print("Error fetching timestamps:", e)


if __name__ == "__main__":

    from datetime import datetime, timedelta
    import random

    DBconnect = DatabaseConnection(db_name='timescaledb')
    conn = DBconnect.get_connection()

    entity = 'getcandledata'
    timescale_operate = TimescaleDBUploader(connection=conn, entity=entity)



    # Function to generate timestamps for a given symbol
    def generate_timestamps(symbol):
        today_date = datetime.now().date()  # Get today's date
        predefined_times = ['17:00:00', '16:59:00', '16:20:00', '16:05:00']  # Predefined times
        
        timestamps = []
        for time_str in predefined_times:
            timestamp = datetime.combine(today_date, datetime.strptime(time_str, '%H:%M:%S').time())  # Combine date with time
            timestamps.append({
                "symbol": symbol,
                "timestamp": timestamp.isoformat()  # Store as ISO format
            })
        
        return timestamps

    # Example usage for each symbol
    symbols = ["AAPL", "GOOG", "MSFT", "AMZN"]

    # Assuming timescale_operate is a previously defined instance of DBUploadOperations
    for symbol in symbols:
        data_to_upload = []
        # Generate timestamps for the symbol
        timestamps = generate_timestamps(symbol)
        
        for record in timestamps:
            # Generate random stock data
            data = {
                "1. open": round(random.uniform(100, 200), 2),
                "2. high": round(random.uniform(200, 300), 2),
                "3. low": round(random.uniform(50, 100), 2),
                "4. close": round(random.uniform(100, 200), 2),
                "5. volume": random.randint(1000, 10000)
            }
            
            candle = {
                "symbol": record["symbol"],
                "timestamp": record["timestamp"],
                "open": data["1. open"],
                "high": data["2. high"],
                "low": data["3. low"],
                "close": data["4. close"],
                "volume": data["5. volume"]
            }

            data_to_upload.append(candle)
        
        # Upload the data to TimescaleDB
        timescale_operate.upload_to_timescaledb(data_to_upload=data_to_upload, metadata=symbol)
