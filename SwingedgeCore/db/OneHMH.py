
import SwingedgeCore.db.Base as base


class OneHMH(base.DBBase): 
    def __init__(self, db_name="timescale"):
        super().__init__(db_name=db_name) 

    def get_results_df(self, query):
        return super().get_results_dataframe(query=query)
