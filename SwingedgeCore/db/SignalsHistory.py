##newer version

from datetime import datetime, timezone
import SwingedgeCore.db.Base as base

class SignalsHistory(base.DBBase):
    """
    Handles insertion of signals into the `signals_history` table.

    Methods:
        add_rows(strategy_id, df, current_date=None): Deletes old data and inserts new records.
    """

    def __init__(self, db_name="timescale"):
        """
        Initializes the database connection.

        Args:
            db_name (str): The database name (default is "timescale").
        """
        super().__init__(db_name=db_name)

    def __execute_query(self, query_template, params=None, bulk=False, fetch_results=False):
        """
        Executes a database query.

        Args:
            query_template (str): SQL query string.
            params (tuple or list, optional): Query parameters.
            bulk (bool): If True, executes bulk insertion.
            fetch_results (bool): If True, fetches query results.

        Returns:
            list or None: Query results if fetch_results is True.
        """
        return super().execute_query(query_template, params, bulk, fetch_results)

    def add_rows(self, strategy_id, df, current_date=None):
        """
        Deletes old records for a strategy on a given date and inserts new records.

        Args:
            strategy_id (int): The strategy ID.
            df (pd.DataFrame): DataFrame containing symbol and cross_trend data.
            current_date (datetime.date, optional): The reference date (defaults to UTC now).
        """
        try:
            # Ensure current_date is set correctly
            if not current_date:
                current_date = datetime.now(timezone.utc).date()

            print(f"🗑️  Attempting to delete existing records for strategy_id={strategy_id}, date={current_date}...")

            # DELETE existing records before inserting new ones
            delete_query = """
            DELETE FROM signals_history
            WHERE strategy_id = %s AND date = %s::date
            RETURNING *;
            """
            
            deleted_rows = self.__execute_query(delete_query, params=(strategy_id, current_date), fetch_results=True)

            if deleted_rows:
                print(f"✅ Deleted {len(deleted_rows)} rows.")
            else:
                print("⚠️ No existing rows found for deletion.")

            # ✅ Commit transaction after DELETE
            self.conn.commit()

            # Prepare unique symbols for insertion
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

            # Insert new records only if there are valid entries
            if unique_symbols:
                insert_query = """
                INSERT INTO signals_history (strategy_id, symbol, date, cross_trend)
                VALUES %s
                """
                bulk_data = list(unique_symbols.values())

                print(f"📥 Inserting {len(bulk_data)} new records...")
                self.__execute_query(insert_query, params=bulk_data, bulk=True)

            print("🎉 Successfully inserted new data.")

        except Exception as e:
            print("❌ Error inserting into signals_history:", e)
            self.conn.rollback()  # Rollback transaction on error to prevent partial inserts






##Older version
# from datetime import datetime, timezone
# import SwingedgeCore.db.Base as base


# class SignalsHistory(base.DBBase):
#     def __init__(self, db_name="timescale"):
#         super().__init__(db_name=db_name)
    
#     def __execute_query(self, query_template, params=None, bulk=False):
#         return super().execute_query(query_template, params, bulk)
    
#     def add_rows(self, strategy_id, df,current_date = None):      
#         try:
#             if not current_date:
#                current_date = datetime.now(timezone.utc).date()
#             else:
#                 current_date = current_date
            
#             delete_query = """
#             DELETE FROM signals_history
#             WHERE strategy_id = %s AND date = %s 
#             """
#             self.__execute_query(delete_query, params=(strategy_id, current_date))

#             unique_symbols = {}
#             for _, row in df.iterrows():
#                 symbol = row['Symbol']
#                 if symbol not in unique_symbols:
#                     unique_symbols[symbol] = (
#                         strategy_id,
#                         symbol,
#                         current_date,
#                         row['cross_trend']
#                     )

#             insert_query = """
#             INSERT INTO signals_history (strategy_id, symbol, date, cross_trend)
#             VALUES %s
#             """
#             bulk_data = list(unique_symbols.values())
#             self.__execute_query(insert_query, params=bulk_data, bulk=True)

#             print("Successfully inserted new data.")
        
#         except Exception as e:
#             print("Error inserting into signals_history: ",e)
