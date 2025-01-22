import SwingedgeCore.db.Base as base
import datetime, timezone

class SignalsHistory(base):
    
    # Delete any existing rows for the same strategy/date combination
    def delete_duplicates(self, sid, date):
        
        delete_query = f"""
            DELETE FROM signals_history
            WHERE strategy_id = {sid} AND date = {date}
            """

        super().execute_single_query(delete_query)
        
    # Add new signals
    def add_rows(self, strategy_id, df):
        
        
        current_date = datetime.now(timezone.utc).date()
        self.delete_duplicates(strategy_id, current_date)
        
        
        # Add new rows
        unique_symbols = {}
        for _, row in df.iterrows():
            symbol = row['Symbol']
            if symbol not in unique_symbols:
                unique_symbols[symbol] = (
                    strategy_id, 
                    symbol, 
                    current_date, 
                    row['Avg_Volume'], 
                    row['cross_trend']
                )

                
        bulk_data = list(unique_symbols.values())
        
        insert_query = """
        INSERT INTO signals_history (strategy_id, symbol, date, avg_volume, cross_trend)
        VALUES ({strategy_id}, {symbol}, {date}, {avg_volume}, {cross_trend})
        """
        super().execute_bulk_query(self, insert_query)

       