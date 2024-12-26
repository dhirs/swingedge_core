import boto3,json,psycopg2

ssm = boto3.client('ssm')
parameter = ssm.get_parameter(Name='timescaledb_credentials', WithDecryption=True)
config = json.loads(parameter['Parameter']['Value'])

connection_established = False

def get_db_connection():
    global connection_established
    try:
        conn = psycopg2.connect(
            database = config['database'],
            user =  config['user'],
            password = config['password'],
            host = config['host'],
            port = config['port'],
        )
        if not connection_established:
            print("Connection to the Timescale PostgreSQL established successfully.")
            connection_established = True  
        
        return conn
    except Exception as e:
        print("Connection to the Timescale PostgreSQL encountered an error: ",e)
        return False
    
def fetch_single_result(*,query, params):
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        print(f"Error executing query: {query}, Error: {e}")
        return None
    finally:
        conn.close()
        
        
def fetch_all_rows(*,query, params):
    global conn  
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            result = cur.fetchall()  
            return result if result else []  
    except Exception as e:
        print("Error executing query in fetch_all_rows: ", e)
        return None
    finally:
        conn.close()
    
    
    
def get_interval_days(start_date, end_date)

    
    d0 = date(start_date)
    d1 = date(end_date)
    delta = d1 - d0
    
    return delta.days
    

