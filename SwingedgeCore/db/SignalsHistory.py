from datetime import datetime, timezone
import SwingedgeCore.db.Base as base


class SignalsHistory(base.DBBase):
    def __init__(self, db_name="timescale"):
        super().__init__(db_name=db_name)
    
    def __execute_query(self, query_template, params=None, bulk=False):
        return super().execute_query(query_template, params, bulk)
    
    def add_rows(self, strategy_id, df):      
        try:
            current_date = datetime.now(timezone.utc).date()
            
            delete_query = """
            DELETE FROM signals_history
            WHERE strategy_id = %s AND date = %s
            """
            self.__execute_query(delete_query, params=(strategy_id, current_date))

            unique_symbols = {}
            for _, row in df.iterrows():
                symbol = row['Symbol']
                if symbol not in unique_symbols:
                    unique_symbols[symbol] = (
                        strategy_id,
                        symbol,
                        current_date,
                        row['cross_trend']
                    )

            insert_query = """
            INSERT INTO signals_history (strategy_id, symbol, date, cross_trend)
            VALUES %s
            """
            bulk_data = list(unique_symbols.values())
            self.__execute_query(insert_query, params=bulk_data, bulk=True)

            print("Successfully inserted new data.")
        
        except Exception as e:
            print("Error inserting into signals_history: ",e)


