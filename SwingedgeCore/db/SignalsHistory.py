import SwingedgeCore.db.Base as base
import datetime, timezone

class SignalsHistory(base):
    
    # Delete any existing rows for the same strategy/date combination
    def delete_duplicates(self, data_to_delete):
        
        delete_query = """
            DELETE FROM signals_history
            WHERE strategy_id = %s AND date = %s
            """

        super().execute_single_query(delete_query,data_to_delete)
        
    # Add new signals
    def add_rows(self, strategy_id, df):
        
        
        current_date = datetime.now(timezone.utc).date()
        
        data_to_delete = [strategy_id,current_date]
        self.delete_duplicates(data_to_delete)
        
        
        # Add new rows
        insert_query = """
        INSERT INTO signals_history (strategy_id, symbol, date, avg_volume, cross_trend)
        VALUES (%s, %s, %s, %s, %s)
        """
        super().execute_bulk_query(self, insert_query,data_to_insert)
        
        # try:
        #     with conn.cursor() as cur:
        #         cur.execute(delete_query, (strategy_id, current_date))
        #         print(f"Deleted all existing data for strategy_id={strategy_id} and date={current_date}.")

            
        #         unique_symbols = {}
        #         for _, row in df.iterrows():
        #             symbol = row['Symbol']
        #             if symbol not in unique_symbols:
        #                 unique_symbols[symbol] = (
        #                     strategy_id, 
        #                     symbol, 
        #                     current_date, 
        #                     row['Avg_Volume'], 
        #                     row['cross_trend']
        #                 )

                
        #         bulk_data = list(unique_symbols.values())
        #         cur.executemany(insert_query, bulk_data)

            
        #         conn.commit()
        #     print("Successfully inserted new data.")
        # except Exception as e:
        #     print("Error while inserting/updating data:", e)
        #     conn.rollback()