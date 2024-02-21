import json
import requests
from datetime import datetime, timedelta
import os
import pandas as pd
import time
from ws_models import sess, engine, Users, WeatherHistory, Locations, UserLocationDay
from common.config_and_logger import config, logger_scheduler

def interpolate_missing_dates_exclude_references(df):
    # Ensure the DataFrame is sorted by date
    df.sort_values(by='date_utc_user_check_in', inplace=True)
    
    # Initialize a list to hold the interpolated rows
    interpolated_rows = []
    
    # Get the current UTC time to use for all interpolated rows
    current_utc_time = datetime.utcnow()
    
    # Iterate through the DataFrame to find gaps and create interpolated rows
    for i in range(len(df) - 1):
        current_row = df.iloc[i]
        next_row = df.iloc[i + 1]
        
        current_date = current_row['date_utc_user_check_in']
        next_date = next_row['date_utc_user_check_in']
        
        # Calculate the gap in days between current and next date
        gap_days = (next_date - current_date).days
        
        # If there is a gap, create the necessary interpolated rows
        for gap_day in range(1, gap_days):
            interpolated_date = current_date + pd.Timedelta(days=gap_day)
            interpolated_row = {
                'user_id': current_row['user_id'],
                'location_id': current_row['location_id'],
                'date_utc_user_check_in': interpolated_date,
                'row_type': 'interpolated',
                'date_time_utc_user_check_in': current_utc_time,
                'time_stamp_utc': current_utc_time
                # Fill other columns as needed, e.g., set to None or default values
            }
            interpolated_rows.append(interpolated_row)
    
    # Convert the list of dictionaries to a DataFrame
    interpolated_df = pd.DataFrame(interpolated_rows)
    
    return interpolated_df



def add_weather_history(location_id, weather_data):
    logger_scheduler.info(f"-- accessed: add_weather_history")
    for day in weather_data['days']:
        weather_history = WeatherHistory(
            location_id=location_id,
            date_time=day['datetime'],
            datetimeEpoch=day['datetimeEpoch'],
            tempmax=day.get('tempmax'),
            tempmin=day.get('tempmin'),
            temp=day.get('temp'),
            feelslikemax=day.get('feelslikemax'),
            feelslikemin=day.get('feelslikemin'),
            feelslike=day.get('feelslike'),
            dew=str(day.get('dew')),
            humidity=str(day.get('humidity')),
            precip=str(day.get('precip')),
            precipprob=str(day.get('precipprob')),
            precipcover=str(day.get('precipcover')),
            preciptype=str(day.get('preciptype')),
            snow=str(day.get('snow')),
            snowdepth=str(day.get('snowdepth')),
            windgust=str(day.get('windgust')),
            windspeed=str(day.get('windspeed')),
            winddir=str(day.get('winddir')),
            pressure=str(day.get('pressure')),
            cloudcover=str(day.get('cloudcover')),
            visibility=str(day.get('visibility')),
            solarradiation=str(day.get('solarradiation')),
            solarenergy=str(day.get('solarenergy')),
            uvindex=str(day.get('uvindex')),
            sunrise=day.get('sunrise'),
            sunriseEpoch=str(day.get('sunriseEpoch')),
            sunset=day.get('sunset'),
            sunsetEpoch=str(day.get('sunsetEpoch')),
            moonphase=str(day.get('moonphase')),
            conditions=day.get('conditions'),
            description=day.get('description'),
            icon=day.get('icon'),
            time_stamp_utc=datetime.utcnow()
        )
        sess.add(weather_history)
    logger_scheduler.info(f"weather_history: {weather_history}")
    # Commit the session to save these objects to the database
    sess.commit()


