import psycopg2, boto3,json
import sqlio

class Base:
      
    def __init__(self):
        try:
            
            ssm = boto3.client("ssm")
            db_config = json.loads(ssm.get_parameter(Name='timescaledb_credentials', WithDecryption=True)['Parameter']['Value'])
            
            self.conn = psycopg2.connect(
                database=db_config["database"],
                user=db_config["user"],
                password=db_config["password"],
                host=db_config["host"],
                port=db_config["port"],
            )
            print("Connected to TimescaleDB.")
           
        except Exception as e:
            print("Error connecting to TimescaleDB: ", e)
            # do some error handling

    def get_results_dataframe(self,query):
        return sqlio.read_sql_query(query,self.conn)
    
    def execute_single_query(self, query):
        conn = self.conn
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
            print("Query executed successfully")
        except Exception as e:
            print("Error while executing query:", e)
            conn.rollback()
            
    def execute_bulk_query(self, query):
        conn = self.conn
        try:
            with conn.cursor() as cur:
                cur.executemany(query)
                conn.commit()
            print("Bulk operation executed successfully")
        except Exception as e:
            print("Error while executing bulk operation:", e)
            conn.rollback()