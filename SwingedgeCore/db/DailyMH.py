import SwingedgeCore.db.Base as base

class DailyMH(base):
    
    def get_results_dataframe(self, q):
        return super().get_results_dataframe(q)
        