import time
from datetime import datetime

class TimeUtils:
    @staticmethod
    def get_current_year_month():
        year_now = time.localtime().tm_year
        month_now = time.localtime().tm_mon
        current_year_month = f"{year_now}-{month_now:02}"
        return current_year_month
    
    @staticmethod
    def convert_utc_to_ist(utc_time):
        india_hour = (utc_time.tm_hour + 5) % 24
        india_minute = (utc_time.tm_min + 30) % 60
        india_second = utc_time.tm_sec

        if utc_time.tm_min + 30 >= 60:
            india_hour = (india_hour + 1) % 24

        return f"{utc_time.tm_year}-{utc_time.tm_mon:02}-{utc_time.tm_mday:02} " \
               f"{india_hour:02}:{india_minute:02}:{india_second:02}"
               
    @staticmethod
    def get_total_duration(start_time: str, end_time: str) -> str:
            start_time_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            end_time_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            
        
            duration = end_time_dt - start_time_dt
            total_seconds = duration.total_seconds()
            
        
            if total_seconds < 0:
                return "Invalid time range: end time is before start time"
            elif total_seconds < 60:
                return f"{int(total_seconds)} seconds"
            elif total_seconds < 3600:
                total_minutes = total_seconds / 60
                if total_minutes.is_integer():
                    return f"{int(total_minutes)} minutes"
                else:
                    return f"{total_minutes:.1f} minutes"
            else:
                total_hours = total_seconds / 3600
                if total_hours.is_integer():
                    return f"{int(total_hours)} hours"
                else:
                    return f"{total_hours:.1f} hours"
