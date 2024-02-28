from apscheduler.schedulers.background import BackgroundScheduler
import json
import requests
from datetime import datetime, timedelta
import os
import pandas as pd
import time
from ws_models import sess, engine, Users, WeatherHistory, Locations, UserLocationDay
from common.config_and_logger import config, logger_scheduler
from ws_utilities import interpolate_missing_dates_exclude_references, \
    add_weather_history


def scheduler_initiator():
    logger_scheduler.info(f'--- Started What Sticks 11 Scheduler ---')

    scheduler = BackgroundScheduler()

    job_ws_weather_and_UserLocationDay_updater = scheduler.add_job(scheduler_manager, 'cron', hour='01', minute='00', second='00')#Production
    # job_ws_weather_and_UserLocationDay_updater = scheduler.add_job(scheduler_manager, 'cron', hour='*', minute='', second='25')#Testing
    # job_call_harmless = scheduler.add_job(harmless, 'cron',  hour='*', minute='03', second='35')#Testing

    scheduler.start()

    while True:
        pass


def harmless():
    # yesterday = datetime.today() - timedelta(days=1)
    # date_formatted = yesterday.strftime('%Y-%m-%d')
    logger_scheduler.info("process started")
    time.sleep(5)  # Wait for 5 seconds
    logger_scheduler.info("process completed")


def scheduler_manager():

    logger_scheduler.info("Start Interpolation")
    interpolate_UserLocationDay_manager()
    logger_scheduler.info("Start Collect Locations")
    logger_scheduler.info("Start Send Visual Crossing API weather reports")
    update_weather_history()
    logger_scheduler.info("--- ENDED What Sticks 11 Scheduler ---")


def interpolate_UserLocationDay_manager():
    users_list = sess.query(Users).all()

    for user in users_list:
        query = sess.query(UserLocationDay).filter(UserLocationDay.user_id == user.id)
        # # Convert the query result to a list of dictionaries
        df_existing_user_locations = pd.read_sql(query.statement, sess.bind)

        if len(df_existing_user_locations) > 1:
            interpolated_df = interpolate_missing_dates_exclude_references(df_existing_user_locations)

            logger_scheduler.info(f"interpolated_df Length: {len(interpolated_df)}")

            if len(interpolated_df) > 0:
                interpolated_df.to_sql('user_location_day', con=engine, if_exists='append', index=False)


def update_weather_history():
    ###########################################################################################
    # This function updates the weather history for yesterday for all locations in Locations ##
    ###########################################################################################
    # api_token = config.VISUAL_CROSSING_TOKEN
    api_token = config.VISUAL_CROSSING_TOKEN
    vc_base_url = config.VISUAL_CROSSING_BASE_URL
    
    # date_time = datetime.strptime(date + " 13:00:00", "%Y-%m-%d %H:%M:%S").isoformat()
    yesterday_date = datetime.utcnow()  - timedelta(days=1)
    yesterday_date_str = yesterday_date.strftime('%Y-%m-%d')
    date_1_start = yesterday_date_str
    # loc = sess.query(Locations).get(location_id)
    locations_list = sess.query(Locations).all()
    weather_hist_call_counter = 0
    for location in locations_list:
        weather_hist_exists = sess.query(WeatherHistory).filter_by(
            location_id = location.id).filter_by(date_time=yesterday_date_str).first()
        
        if not weather_hist_exists:
            weather_hist_call_counter += 1
            lat = location.lat
            lon = location.lon
            vc_weather_history_api_call_url = f"{vc_base_url}/{str(lat)},{str(lon)}/{date_1_start}?unitGroup=metric&key={api_token}&include=days"
            request_vc_weather_history = requests.get(vc_weather_history_api_call_url)
            # logger_scheduler.info(f"VC API call vc_weather_history_api_call_url: {vc_weather_history_api_call_url} ")
            # logger_scheduler.info(f"VC API call status_code: {request_vc_weather_history.status_code} ")
            if request_vc_weather_history.status_code == 200:
                # logger_scheduler.info(f"VC API call success! ")
                weather_data = request_vc_weather_history.json()
                add_weather_history(location.id, weather_data)
    
    logger_scheduler.info(f"called VC API for {weather_hist_call_counter} weather history locations ")

if __name__ == '__main__':  
    scheduler_initiator()