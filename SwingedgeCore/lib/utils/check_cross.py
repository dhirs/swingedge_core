# class checkCross:
#     # method, three flags, window
#     # df,direction = 'Both','Up','Down'
#     pass

import pandas_ta as ta
import pandas as pd
from datetime import datetime,timezone,timedelta

class checkCross:
    def __init__(self, df, window=3, direction='Up',period='weeks'):
        self.df = df
        self.scan_period = window
        self.direction = direction
        self.time = period
    
    def __filter_data_within_specified_period(self,df,cross_period):
        try:
            df['bucket'] = pd.to_datetime(df['bucket'], utc=True)
            current_date = datetime.now(timezone.utc)

            if self.time.lower() == 'weeks':
                days = cross_period * 7
                cutoff_date = current_date - timedelta(days=days)
            elif self.time.lower() == 'days':
                cutoff_date = current_date - timedelta(days=cross_period)
            elif self.time.lower() == 'months':
                days = cross_period * 30  
                cutoff_date = current_date - timedelta(days=days)
            elif self.time.lower() == 'hours':
                cutoff_date = current_date - timedelta(hours=cross_period)
            else:
                raise ValueError("Invalid period type. Choose from 'weeks', 'days', 'months', or 'hours'.")

            cutoff_date = cutoff_date.replace(hour=0, minute=0, second=0, microsecond=0)
            filtered_df = df[df['bucket'] >= cutoff_date]
            return filtered_df

        except Exception as e:
            print(f"Error filtering data for the last week: {e}")
            return pd.DataFrame() 
        
    def scan_macd(self) -> list:
        data = self.df
        data = self.__filter_data_within_specified_period(df=data, cross_period=self.scan_period)
        all_symbols = []
        try:
            macd_symbols_up, macd_symbols_down = [], []
            
            # Loop over each unique symbol in the dataset
            for symbol in data['symbol'].unique():
                cross_data = data[data['symbol'] == symbol]
                cross_list = list(cross_data['cross'])
                macd_line_status_list = list(cross_data['MACD_line_status'])
                # print(symbol,'--',cross_list,'--',macd_line_status_list)

                # Find the index of the last cross '1' in the cross column
                last_cross_index = len(cross_list) - 1 - cross_list[::-1].index(1) if 1 in cross_list else -1

                if last_cross_index != -1:  # Only process if there's a '1' in the cross column
                    # Get the list of MACD line statuses after the last cross
                    trend_statuses = macd_line_status_list[last_cross_index + 1:] 

                    # Check if all are "Up"
                    if all(status == "Up" for status in trend_statuses):
                        macd_symbols_up.append(symbol)
                    # Check if all are "Down"
                    elif all(status == "Down" for status in trend_statuses):
                        macd_symbols_down.append(symbol)
                    # Handle mixed cases
                    else:
                        # If the last status is "Down", append to symbols_down
                        if trend_statuses[-1] == "Down":
                            macd_symbols_down.append(symbol)
                        # If the last status is "Up", append to symbols_up
                        else:
                            macd_symbols_up.append(symbol)
                # If there's no cross '1' in the cross column, we do not append the symbol
                else:
                    continue
                
            all_symbols.append(macd_symbols_up)
            all_symbols.append(macd_symbols_down)
            
            if self.direction.lower() == "up":
                return macd_symbols_up
            elif self.direction.lower() == "down":
                return macd_symbols_down
            elif self.direction.lower() == "both":
                return all_symbols
            else:
                raise ValueError("Not a valid input")
        except Exception as e:
            print("Error getting macd symbols: ", e)