##importing packages
import requests,os
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import pandas as pd
import base64
import logging
from datetime import datetime, timezone
import boto3,json,psycopg2



# logger
log_directory = "logging"
os.makedirs(log_directory, exist_ok=True)
    
logging.basicConfig(
    filename=os.path.join(log_directory, 'scraping_errors.log'), 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()



# SSM params
region = "ap-south-1"
ssm = boto3.client("ssm")
db_config = json.loads(ssm.get_parameter(Name='timescaledb_credentials', WithDecryption=True)['Parameter']['Value'])


# Connect to the database
def get_db_connection():
    try:
        conn = psycopg2.connect(
            database=db_config["database"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"],
        )
        print("Connected to TimescaleDB.")
        return conn
    except Exception as e:
        print("Error connecting to TimescaleDB: ", e)
        return None

conn = get_db_connection()


ua = UserAgent()


def fetch_symbols_from_db():
    current_timestamp = datetime.now(timezone.utc).date()
    try:
        print(f"Timestamp: {current_timestamp}")
        
        query = """
            SELECT symbol FROM signals_history
            WHERE date = %s   
        """


        with conn.cursor() as cursor:
            cursor.execute(query, (current_timestamp,))
            result = cursor.fetchall()

        if result:
            symbols = [symbol_info [0] for symbol_info in result] 
            return symbols
        else:
            print("No payload found for the given timestamp.")
            return None

    except Exception as e:
        print("Error fetching payload from DB:", e)
        return None

     
     
     
     
def scrap_url(symbols):
    timestamp = datetime.now(timezone.utc)  
    all_data = [] 
    blocked_symbols = [] 
    
    for symbol in symbols:
        try:
            headers = {
                "User-Agent": ua.random
            }

            url = f"https://finance.yahoo.com/quote/{symbol}/holders/"

            print(f"Fetching data for symbol: {symbol}")
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                print("Successfully fetched the page!")
                soup = BeautifulSoup(response.text, "html.parser")

               
                title = soup.title.text
                print("Page Title:", title)

                
                tables = soup.find_all("table")

                if tables:
                    first_table = tables[0]
                    table_data = []  

                    rows = first_table.find_all("tr")
                    for row in rows:
                        cells = row.find_all("td")
                        if len(cells) == 2: 
                            key = cells[1].text.strip()  
                            value = cells[0].text.strip()  
                            table_data.append({"description": key, "value": value})

                    all_data.append({
                        "symbol": symbol,
                        "timestamp": timestamp,
                        "table_data": table_data
                    })
                else:
                    print(f"No tables found for symbol {symbol}")

            else:
                print(f"Failed to fetch the page for {symbol}. Status code: {response.status_code}")
                blocked_symbols.append(symbol)

        except Exception as e:
            print(f"Error fetching data for symbol {symbol}: {e}")
            blocked_symbols.append(symbol)
            
    if blocked_symbols:
        logger.error(f"Blocked symbols: {blocked_symbols}")
        
    return all_data



def insert_into_table(df):
    cursor = conn.cursor()

    upsert_query = """
    INSERT INTO i_holding (symbol, date, p_insider, p_inst, n_inst)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (symbol) 
    DO UPDATE 
    SET date = EXCLUDED.date,
        p_insider = EXCLUDED.p_insider,
        p_inst = EXCLUDED.p_inst,
        n_inst = EXCLUDED.n_inst;
    """
    
    try:
        new_symbols = set(df['symbol'])

        delete_query = """
        DELETE FROM i_holding 
        WHERE symbol NOT IN %s;
        """
        cursor.execute(delete_query, (tuple(new_symbols),)) 
        print(f"Deleted records for symbols not in the new list.")


        upsert_data = []

        for index, row in df.iterrows():
            symbol = row['symbol']
            date = row['date']
            p_insider = float(row['percentage_of_Shares_Held_by_All_Insider'].strip('%'))  
            p_inst = float(row['percentage_of_shares_by_institutions'].strip('%'))  
            n_inst = int(row['number_of_institutions_holding_the_stock'])  

            upsert_data.append((symbol, date, p_insider, p_inst, n_inst))


        cursor.executemany(upsert_query, upsert_data)
        print(f"Inserted/Updated {len(upsert_data)} records.")

        conn.commit()

    except Exception as e:
        print(f"Error upserting data into i_holding table: {e}")
        conn.rollback()
    finally:
        cursor.close()



def scrap_data_to_csv(data):
    date_list = []
    symbol_list = []
    insider_list = []
    percentage_institution_list = []
    number_institution_list = []

    scrap_data = {}
    try:
        for info in data:
            sym = info['symbol']
            dt = info['timestamp'].strftime('%Y-%m-%d %H:%M:%S') 
            insider = info['table_data'][0]['value']
            per_ins = info['table_data'][1]['value'] 
            num_ins  = info['table_data'][3]['value']
            
            if insider == '--':
                insider = '0'
            if per_ins == '--':
                per_ins = '0'  
            if num_ins == '--':
                num_ins = '0'  

            
            date_list.append(dt)
            symbol_list.append(sym)
            insider_list.append(insider)
            percentage_institution_list.append(per_ins)
            number_institution_list.append(num_ins)

      
        scrap_data = {
            'date': date_list,
            'symbol': symbol_list,
            'percentage_of_Shares_Held_by_All_Insider': insider_list,
            'percentage_of_shares_by_institutions': percentage_institution_list,
            'number_of_institutions_holding_the_stock': number_institution_list
        }

        df = pd.DataFrame(scrap_data)

        print(df)

        insert_into_table(df)
        
        return df

    except Exception as e:
        print("Error: ", e)
       
       
       
        
def execute_strategy():
    try:
            symbols = fetch_symbols_from_db()
            data = scrap_url(symbols)
            scrap_data_to_csv(data=data)
    except Exception as e:
        print("Error:",e)
    
if __name__ == "__main__":
    execute_strategy()

    
