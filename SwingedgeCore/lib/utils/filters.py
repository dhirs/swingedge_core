class Filters:
     def __init__(self,df):
         self.df = df
         
         
     def close_price_filter(self,min_price=None,max_price=None):
        data = self.df
        unique_symbols = data['symbol'].unique()
        
                
        for symbol in unique_symbols:
            symbol_data = data[data['symbol'] == symbol]
            
            min_timestamp_row = symbol_data.loc[symbol_data['bucket'].idxmin()]
            max_timestamp_row = symbol_data.loc[symbol_data['bucket'].idxmax()]
            
            # Minimum and maximum both specified
            if min_price is not None and max_price is not None:
                if not (min_price <= min_timestamp_row['close'] <= max_price):
                    data = data[data['symbol'] != symbol]
                
            # Minimum but no maximum
            if min_price is not None and max_price is None:
                if not (min_price <= min_timestamp_row['close']):
                    data = data[data['symbol'] != symbol]
                
            # Maximum but no minimum
            if min_price is None and max_price is not None:
                if not (max_price <= max_timestamp_row['close']):
                    data = data[data['symbol'] != symbol]
                
        data = data.reset_index(drop=True)
        return data