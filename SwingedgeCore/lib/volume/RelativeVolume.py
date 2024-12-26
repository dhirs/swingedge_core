from AverageDailyVolume import get_average_daily_volume
from TotalCurrentVolume import get_total_current_volume

def get_relative_volume(symbol,period):
    try:
        relative_volume = get_total_current_volume(symbol=symbol)/get_average_daily_volume(symbol=symbol,n=period)
        relative_volume = round(relative_volume,7)
        return relative_volume
    except Exception as e:
        print(f"Issue calculating relative volume: {e}")


if __name__ == "__main__":
    symbol = 'IBM'
    n = 3
    print(f"Result for {symbol}: {get_relative_volume(symbol, n)}")
