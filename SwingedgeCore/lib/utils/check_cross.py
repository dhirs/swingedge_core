# class checkCross:
#     # method, three flags, window
#     # df,direction = 'Both','Up','Down'
#     pass

# class checkCross:
#     # method, three flags, window
#     # df,direction = 'Both','Up','Down'
#     pass

import pandas as pd
from datetime import datetime, timezone, timedelta


class checkCross:
    """
    A class to detect MACD crossovers and identify trends based on a specified period.

    Attributes:
        df (pd.DataFrame): Data containing MACD values and cross signals.
        scan_period (int): The number of days, weeks, or months to scan for trend detection.
        direction (str): The direction of the trend to detect ("up", "down", or "both").
        period (str): The period type ("days", "weeks", or "months").
        current_date (datetime): The reference date for filtering data.
    """

    def __init__(self, df, scan_period=3, direction=None, period="days", current_date=None):
        """
        Initializes the checkCross class with the given parameters.

        Args:
            df (pd.DataFrame): The input data frame containing stock/macd values.
            scan_period (int, optional): The period over which trends are checked. Default is 3.
            direction (str, optional): The trend direction to check ("up", "down", "both"). Default is None.
            period (str, optional): The type of period ("days", "weeks", "months"). Default is "days".
            current_date (str or datetime, optional): The current reference date. If a string, it's converted to datetime.
        """
        self.df = df
        self.scan_period = scan_period
        self.direction = direction
        self.period = period  # Ensure period is a string
        
        # Ensure current_date is a proper datetime object with UTC timezone
        if isinstance(current_date, str):  
            self.current_date = datetime.strptime(current_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        else:
            self.current_date = datetime.now(timezone.utc)

    def __filter_data_within_specified_period(self):
        """
        Filters the dataframe to include only the rows within the specified scan period. 📊

        Returns:
            pd.DataFrame: The filtered dataframe within the specified scan period.
        """
        # Determine the time delta based on the selected period
        if self.period == "days":
            time_delta = timedelta(days=self.scan_period)
        elif self.period == "weeks":
            time_delta = timedelta(weeks=self.scan_period)
        elif self.period == "months":
            time_delta = timedelta(days=self.scan_period * 30)  # Approximate
        else:
            raise ValueError("❌ Invalid period. Use 'days', 'weeks', or 'months'.")
        
        # Compute the start date for filtering
        start_date = self.current_date - time_delta
        
        # Ensure the DataFrame has a proper datetime column with UTC timezone
        self.df["timestamp"] = pd.to_datetime(self.df.iloc[:, 1], utc=True)
        
        # Create a separate date-only column for filtering
        self.df["date"] = self.df["timestamp"].dt.date
        
        # Apply filtering based on the date range
        filtered_df = self.df[(self.df["date"] >= start_date.date()) & (self.df["date"] <= self.current_date.date())]
        
        # Logging the filter operation
        print("📅 Min Date in DF:", self.df["date"].min())
        print("📅 Max Date in DF:", self.df["date"].max())
        print("🔍 Start Date for Filtering:", start_date.date())
        print("🕒 Current Date for Filtering:", self.current_date.date())
        
        return filtered_df

    def detect_trend(self):
        """
        Detects trend direction based on MACD crossover signals. 📈📉

        Returns:
            list: A list of symbols in an uptrend, downtrend, or both.
        """
        data = self.__filter_data_within_specified_period()  # Get filtered data

        uptrend = []   # List to store uptrend symbols
        downtrend = [] # List to store downtrend symbols

        try:
            for symbol in data['symbol'].unique():
                uptrend_cross_flag = None
                downtrend_cross_flag = None
                
                # Extract data for the current symbol
                cross_data = data[data['symbol'] == symbol]

                # Extract MACD and signal line values
                uptrend_cross_list = list(cross_data['uptrend_cross'])
                downtrend_cross_list = list(cross_data['downtrend_cross'])
                blue_line = list(cross_data['MACD_12_26_9'])  # MACD Line
                orange_line = list(cross_data['MACDs_12_26_9'])  # Signal Line

                # Identify the first occurrence of an uptrend cross
                for i in range(len(uptrend_cross_list)):
                    if uptrend_cross_list[i] == 1:
                        uptrend_cross_flag = i
                        break  # Stop after finding the first cross

                # Identify the first occurrence of a downtrend cross
                for i in range(len(downtrend_cross_list)):
                    if downtrend_cross_list[i] == 1:
                        downtrend_cross_flag = i
                        break  

                # Check for uptrend continuation after crossover
                if uptrend_cross_flag is not None:
                    if all((blue_line[i] < 0 and orange_line[i] < 0) and (blue_line[i] > orange_line[i]) for i in range(uptrend_cross_flag, len(blue_line))):
                        uptrend.append(symbol)

                # Check for downtrend continuation after crossover
                if downtrend_cross_flag is not None:   
                    if all((blue_line[i] > 0 and orange_line[i] > 0) and (blue_line[i] < orange_line[i]) for i in range(downtrend_cross_flag, len(blue_line))):
                        downtrend.append(symbol)

            # Log detected trends
            print(f"📈 Uptrend Symbols: {uptrend}")
            print(f"📉 Downtrend Symbols: {downtrend}")

            # Return results based on user-selected direction
            if self.direction.lower() == "up":
                return uptrend
            elif self.direction.lower() == "down":
                return downtrend
            elif self.direction.lower() == "both":
                return [uptrend, downtrend]
            else:
                raise ValueError("❌ Not a valid direction input. Use 'up', 'down', or 'both'.")

        except Exception as e:
            print("❌ Error in detecting trend:", e)
            return []
