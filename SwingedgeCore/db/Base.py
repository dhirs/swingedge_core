import psycopg2
import psycopg2.extras
from SwingedgeCore.cloud.aws.client.ssm import Credentials
import pandas.io.sql as sqlio

class DBBase:
    def __init__(self, db_name="timescale"):
        self.db_name = db_name
        self.conn = self.__get_timescaledb_connection()

    def __get_timescaledb_connection(self):
        cred = Credentials(credential_name=self.db_name)
        db_config = cred.get_credentials()
        try:
            conn = psycopg2.connect(
                database=db_config['database'],
                user=db_config['user'],
                password=db_config['password'],
                host=db_config['host'],
                port=db_config['port'],
            )
            print("✅ Connection to the Timescale PostgreSQL established successfully.")
            return conn
        except Exception as e:
            print("❌ Connection to the Timescale PostgreSQL encountered an error: ", e)
            return None

    def get_results_dataframe(self, query):
        try:
            df = sqlio.read_sql_query(query, self.conn)
            return df
        except Exception as e:
            raise ValueError("❌ No dataframe results found: ", e)

    def execute_query(self, query_template, params=None, bulk=False, fetch_results=False):
        conn = self.conn
        try:
            with conn.cursor() as cur:
                if bulk:
                    psycopg2.extras.execute_values(cur, query_template, params)
                else:
                    cur.execute(query_template, params)

                
                if fetch_results:
                    results = cur.fetchall()
                    return results

                conn.commit()
                print("✅ Query executed successfully.")
        except Exception as e:
            print("❌ Error while executing query:", e)
            conn.rollback()
            return None
        
