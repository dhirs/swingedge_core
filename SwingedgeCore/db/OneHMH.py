
import SwingedgeCore.db.Base as base

class OneHMH(base):
    def get_results_df(query):
        
        data = sqlio.read_sql_query(query,conn)
        return data
