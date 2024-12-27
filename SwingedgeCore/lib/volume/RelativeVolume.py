from AverageDailyVolume import get_average_daily_volume
from TotalVolume import get_total_current_volume

def get_relative_volume(*,symbol,n,hour_type='eh'):
    relative_volume = get_total_current_volume(symbol=symbol,hour_type=hour_type)/get_average_daily_volume(symbol=symbol,n=n,hour_type=hour_type)
    relative_volume = round(relative_volume,7)
    return relative_volume


if __name__ == "__main__":
    hour_type = 'mh'
    symbol = 'IBM'
    period = 3
    print(f"Result for {symbol}: {get_relative_volume(symbol=symbol, n=period, hour_type=hour_type)}")
    
##RV more than 3? List all stocks where RV is more than 3?
##market hours(9:30m to 4:00pm)[optional] and extended hours(4:00am to 8:00pm)[default]
##eh(extendedhours)=1{default}, mh=1(9:30m to 4:00pm)
##pass the flag(mh/eh) db.py