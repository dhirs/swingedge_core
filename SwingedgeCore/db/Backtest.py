##Us etfs stocks
import SwingedgeCore.db.Base as base


class Backtest(base.DBBase): 
    def __init__(self, db_name="timescale"):
        super().__init__(db_name=db_name) 

    def get_results_df(self, query):
        return super().get_results_dataframe(query=query)
    
    def execute_query(self, query_template, params=None, bulk=False, fetch_results=True):
        return super().execute_query(query_template, params, bulk, fetch_results)