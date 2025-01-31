import pandas as pd

class Filters:
    def __init__(self, df):
        self.df = df

    def close_price_filter(self, min_price=None, max_price=None, price_reference='min'):
        """
        Filters rows based on their closing price at either the earliest or latest timestamp.

        :param min_price: Minimum closing price threshold (inclusive).
        :param max_price: Maximum closing price threshold (inclusive).
        :param price_reference: Determines whether to use the price at the 'min' (earliest) or 'max' (latest) timestamp.
        :return: Filtered DataFrame.
        """
        data = self.df.copy()
        price_reference = price_reference.lower()

        if price_reference not in ['min', 'max']:
            raise ValueError("price_reference must be 'min' or 'max'.")

        # Find the row corresponding to the earliest or latest timestamp for each symbol
        if price_reference == 'min':
            ref_rows = data.loc[data.groupby("symbol")["bucket"].idxmin()]
        else:
            ref_rows = data.loc[data.groupby("symbol")["bucket"].idxmax()]

        # Apply filtering conditions in one step
        if min_price is not None and max_price is not None:
            ref_rows = ref_rows[(ref_rows['close'] >= min_price) & (ref_rows['close'] <= max_price)]
        elif min_price is not None:
            ref_rows = ref_rows[ref_rows['close'] >= min_price]
        elif max_price is not None:
            ref_rows = ref_rows[ref_rows['close'] <= max_price]
        
        # Merge back to retain only rows that match the filtered symbols
        filtered_data = data[data['symbol'].isin(ref_rows['symbol'])].reset_index(drop=True)

        return filtered_data