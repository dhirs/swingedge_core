import psycopg2
import boto3
import SwingedgeCore.cloud.aws.credentials as creds

def get_creds()

    return creds.get_db_credentials():

def get_connection():
    config = get_creds()
    
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

conn = get_connection()

def get_one(query,params):
    
    global conn
    
    try:
        with conn.cursor() as cur:
        cur.execute(query,params)
        result = cur.fetchone()
        return result[0] if result else None

    except Exception as e:
        return e
    

