##Us etfs stocks
import SwingedgeCore.db.Base as base


class UsEtfsStocksIntraday(base.DBBase): 
    def __init__(self, db_name="timescale"):
        super().__init__(db_name=db_name) 

    def get_results_df(self, query):
        return super().get_results_dataframe(query=query)
    
    def execute_query(self, query_template, params=None, bulk=True, fetch_results=False):
        return super().execute_query(query_template, params, bulk, fetch_results)