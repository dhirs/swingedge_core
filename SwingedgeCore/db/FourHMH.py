import SwingedgeCore.db.Base as base

class FourHMH(base):
    
    def get_results_df(query):
    
        data = sqlio.read_sql_query(query,conn)
        return data

