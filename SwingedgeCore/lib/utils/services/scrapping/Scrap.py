import requests, os
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import pandas as pd
import logging
from datetime import datetime, timezone
import SwingedgeCore.db.Base as base


class Scrap(base.DBBase):
    def __init__(self, db_name="timescale",strategy_id=1,current_date=None):
        self.strategy_id = strategy_id
        self.current_date = current_date
        super().__init__(db_name=db_name)

    def __execute_query(self, query_template, params=None, bulk=False, fetch_results=True):
        return super().execute_query(query_template, params, bulk, fetch_results)

    def __fetch_symbols_from_db(self):
        if not self.current_date:
           current_timestamp = datetime.now(timezone.utc).date()
        else:
            current_timestamp = self.current_date
        try:
            print(f"Timestamp: {current_timestamp}")

            query = """
                SELECT symbol FROM signals_history
                WHERE date = %s AND strategy_id = %s
            """
            params_data = (current_timestamp,self.strategy_id,)
            result = self.__execute_query(query, params=params_data)
            
            if result:
                symbols = [symbol_info[0] for symbol_info in result]
                return symbols
            else:
                print("No payload found for the given timestamp.")
                return None

        except Exception as e:
            print("Error fetching symbols from DB:", e)
            return None

    def __get_logger(self):
        log_directory = "logging"
        os.makedirs(log_directory, exist_ok=True)
            
        logging.basicConfig(
            filename=os.path.join(log_directory, 'scraping_errors.log'), 
            level=logging.DEBUG, 
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger()

    def __scrap_url(self, symbols):
        ua = UserAgent()
        timestamp = datetime.now(timezone.utc)  
        all_data = [] 
        blocked_symbols = [] 
        logger = self.__get_logger()  
        
        for symbol in symbols:
            try:
                headers = {"User-Agent": ua.random}
                url = f"https://finance.yahoo.com/quote/{symbol}/holders/"

                print(f"Fetching data for symbol: {symbol}")
                logger.info(f"Fetching data for symbol: {symbol}")
                response = requests.get(url, headers=headers)

                if response.status_code == 200:
                    print("Successfully fetched the page!")
                    soup = BeautifulSoup(response.text, "html.parser")
                    title = soup.title.text if soup.title else "No title available"
                    logger.info(f"Successfully fetched the page for symbol: {symbol}")
                    logger.debug(f"Page Title: {title}")
                    
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

    def execute(self):
        symbols = self.__fetch_symbols_from_db()
        if not symbols:
            print("No symbols to process. Exiting.")
            return
        
        data = self.__scrap_url(symbols=symbols)
        date_list, symbol_list, insider_list, percentage_institution_list, number_institution_list, strategy_list = [], [], [], [], [], []

        try:
            for info in data:
                sym = info['symbol']
                dt = info['timestamp'].strftime('%Y-%m-%d %H:%M:%S') 
                table_data = info['table_data']

                insider = table_data[0]['value'] if table_data[0]['value'] != '--' else '0'
                per_ins = table_data[1]['value'] if table_data[1]['value'] != '--' else '0'
                num_ins = table_data[3]['value'] if table_data[3]['value'] != '--' else '0'

                date_list.append(dt)
                symbol_list.append(sym)
                insider_list.append(insider)
                percentage_institution_list.append(per_ins)
                number_institution_list.append(num_ins)
                strategy_list.append(self.strategy_id)

            scrap_data = {
                'date': date_list,
                'symbol': symbol_list,
                'percentage_of_Shares_Held_by_All_Insider': insider_list,
                'percentage_of_shares_by_institutions': percentage_institution_list,
                'number_of_institutions_holding_the_stock': number_institution_list,
                'strategy_id': strategy_list
            }

            df = pd.DataFrame(scrap_data)
            print(df)
            self.__insert_into_table(df=df)
        except Exception as e:
            print("Error during data processing:", e)

    
    
    def __insert_into_table(self, df):
        upsert_query = """
        INSERT INTO i_holding (symbol, date, p_insider, p_inst, n_inst, strategy_id)
        VALUES %s
        ON CONFLICT (symbol,date,strategy_id) 
        DO UPDATE 
        SET date = EXCLUDED.date,
            p_insider = EXCLUDED.p_insider,
            p_inst = EXCLUDED.p_inst,
            n_inst = EXCLUDED.n_inst;
        """
        try:
            # Get the current UTC date
            current_date = datetime.now(timezone.utc).date()

            # Step 1: Delete records for today's date
            delete_today_query = """
            DELETE FROM i_holding
            WHERE DATE(date) = %s and strategy_id = %s;
            """
            
            params_data = (current_date,self.strategy_id,)
            self.__execute_query(query_template=delete_today_query, params=params_data, bulk=False, fetch_results=False)
            print(f"Deleted records for today's date and strategy: {current_date} | strategy_id: {self.strategy_id}.")

            # Step 2: Prepare data for insertion/upsertion
            upsert_data = [
                (
                    row['symbol'],
                    row['date'],
                    float(row['percentage_of_Shares_Held_by_All_Insider'].replace(',', '').strip('%')),  
                    float(row['percentage_of_shares_by_institutions'].replace(',', '').strip('%')),  
                    int(row['number_of_institutions_holding_the_stock'].replace(',', '')),  
                    row['strategy_id'],
                )
                for _, row in df.iterrows()
            ]
            # Step 3: Insert/Update new data
            self.__execute_query(query_template=upsert_query, params=upsert_data, bulk=True, fetch_results=False)
            print(f"Inserted/Updated {len(upsert_data)} records.")

        except Exception as e:
            print(f"Error upserting data into i_holding table: {e}")



if __name__ == "__main__":
    scrap = Scrap(strategy_id=1)
    scrap.execute()