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



##DB Timestamps
class DBTimestamps:
    def __init__(self, db_connection):
        if not db_connection:
            raise ValueError("Database connection is required.")
        self.connection = db_connection

    def fetch_recent_timestamps(self):
        try:
            query = """
                SELECT symbol, MAX(timestamp) as latest_timestamp
                FROM stock_dummy
                GROUP BY symbol
                ORDER BY latest_timestamp DESC
            """
            with self.connection.cursor() as cur:
                cur.execute(query)
                raw_results = cur.fetchall()

            if not raw_results:
                print("No results found.")
                return []

            formatted_results = [
                {'symbol': row[0], 'timestamp': row[1].strftime('%Y-%m-%d %H:%M:%S')}
                for row in raw_results
            ]

            print(f"Recent timestamps fetched successfully. Total: {len(formatted_results)}")
            return formatted_results

        except Exception as e:
            print(f"Error fetching timestamps: {e}")
            return []



##Generic DB operations
class GenericDBOperations:
    def __init__(self, connection, entity):
        self.connection = connection
        self.entity = entity
        if not self.connection:
            raise ValueError("Cannot establish connection with DB")
        self.table_name = self.__resolve_entity()
        
    def __resolve_entity(self):
        entity_table_map = {
            "getcandledata": "us_etfs_stocks"
        }
        table_name = entity_table_map.get(self.entity.lower())
        if not table_name:
            raise ValueError(f"Entity '{self.entity}' is not recognized.")
        return table_name  

    def __insertion_operation(self, query_template, metadata, data_to_upload=None):
        if not data_to_upload:
            print(f"No data to upload for symbol={metadata}.")
            return False
        
        try:
            # Extract column names dynamically
            columns = list(data_to_upload[0].keys())
            column_names = ", ".join(columns)
            query_values = [tuple(record.values()) for record in data_to_upload]
            
            # Format the query with column names
            formatted_query = query_template.format(column_names=column_names)

            # Execute query using execute_values for batch insert/update
            with self.connection.cursor() as cur:
                execute_values(cur, formatted_query, query_values)
                self.connection.commit()

            print(f"Data successfully upserted into table '{self.table_name}' for symbol={metadata}.")
            return True
        except Exception as e:
            print(f"Error executing INSERT/UPDATE/UPSERT query: {e}")
            return False
        
    def __non_insertion_operation(self, query_template, parameters=None, fetch_results=True, autocommit=False):
        try:
            # Handle autocommit for queries like CALL
            if autocommit:
                self.connection.autocommit = True
                print("Executing call aggregates!")

            with self.connection.cursor() as cur:
                if parameters:
                    cur.execute(query_template, parameters)
                else:
                    cur.execute(query_template)

                # Fetch results for SELECT queries
                if fetch_results:
                    results = cur.fetchall()
                    print(f"Query executed successfully. Retrieved {len(results)} rows.")
                    return results

                # Commit transaction for non-SELECT queries
                if not autocommit:
                    self.connection.commit()
                print("Query executed successfully. No results to fetch.")
                return True
        except Exception as e:
            print(f"Error executing query: {e}")
            return False
        finally:
            if autocommit:
                self.connection.autocommit = False  # Restore default behavior

    def execute_operation(self, query_template, data_to_upload=None, metadata=None, parameters=None):
        query_template = query_template.replace("{table_name}", self.table_name)
        
        try:
            # Identify query type and execute accordingly
            if any(keyword in query_template.lower() for keyword in ["insert", "update", "upsert"]):
                return self.__insertion_operation(
                    query_template=query_template, 
                    data_to_upload=data_to_upload, 
                    metadata=metadata
                )
            elif "call" in query_template.lower():
                # Handle CALL queries in autocommit mode
                return self.__non_insertion_operation(
                    query_template=query_template, 
                    parameters=parameters, 
                    fetch_results=False, 
                    autocommit=True
                )
            else:
                # Handle SELECT and other non-insertion queries
                fetch_results = "select" in query_template.lower()
                return self.__non_insertion_operation(
                    query_template=query_template, 
                    parameters=parameters, 
                    fetch_results=fetch_results
                )
        except Exception as e:
            print(f"Error executing operation: {e}")
            return False


if __name__ == "__main__":

    from datetime import datetime, timedelta
    import random

    DBconnect = DatabaseConnection(db_name='timescaledb')
    conn = DBconnect.get_connection()

    entity = 'getcandledata'
    # timescale_operate = TimescaleDBUploader(connection=conn, entity=entity)
    timescale_operate = GenericDBOperations(connection=conn, entity=entity)
    query_template = """
    INSERT INTO {table_name} ({column_names})
    VALUES %s
    ON CONFLICT (symbol, timestamp)
    DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume;
    """


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
        # timescale_operate.upload_to_timescaledb(data_to_upload=data_to_upload, metadata=symbol)
        timescale_operate.execute_operation(query_template=query_template,data_to_upload=data_to_upload, metadata=symbol)
