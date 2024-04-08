from apscheduler.schedulers.background import BackgroundScheduler
import json
import requests
from datetime import datetime, timedelta
import os
import pandas as pd
import time
from ws_models import engine, DatabaseSession, Users, WeatherHistory, Locations, UserLocationDay
from common.config_and_logger import config, logger_scheduler
from common.utilities import wrap_up_session
from ws_utilities import interpolate_missing_dates_exclude_references, \
    add_weather_history


def scheduler_initiator():
    logger_scheduler.info(f'--- Started What Sticks 11 Scheduler ---')

    scheduler = BackgroundScheduler()

    # job_ws_weather_and_UserLocationDay_updater = scheduler.add_job(scheduler_manager, 'cron', day='*', hour='01', minute='00', second='00')#Production
    job_ws_weather_and_UserLocationDay_updater = scheduler.add_job(scheduler_manager, 'cron', hour='*', minute='25', second='55')#Testing
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

    logger_scheduler.info("Start Send Visual Crossing API weather reports")
    update_weather_history()
    logger_scheduler.info("--- ENDED What Sticks 11 Scheduler ---")


def interpolate_UserLocationDay_manager():
    db_session = DatabaseSession()
    users_list = db_session.query(Users).all()

    for user in users_list:
        query = db_session.query(UserLocationDay).filter(UserLocationDay.user_id == user.id)
        # # Convert the query result to a list of dictionaries
        df_existing_user_locations = pd.read_sql(query.statement, db_session.bind)

        if len(df_existing_user_locations) > 1:
            interpolated_df = interpolate_missing_dates_exclude_references(df_existing_user_locations)

            logger_scheduler.info(f"interpolated_df Length: {len(interpolated_df)}")

            if len(interpolated_df) > 0:
                interpolated_df.to_sql('user_location_day', con=engine, if_exists='append', index=False)
    
    wrap_up_session(db_session)


def update_weather_history():
    ###########################################################################################
    # This function updates the weather history for yesterday for all locations in Locations ##
    ###########################################################################################
    # api_token = config.VISUAL_CROSSING_TOKEN
    api_token = config.VISUAL_CROSSING_TOKEN
    vc_base_url = config.VISUAL_CROSSING_BASE_URL
    db_session = DatabaseSession()
    
    # date_time = datetime.strptime(date + " 13:00:00", "%Y-%m-%d %H:%M:%S").isoformat()
    yesterday_date = datetime.utcnow()  - timedelta(days=1)
    yesterday_date_str = yesterday_date.strftime('%Y-%m-%d')
    date_1_start = yesterday_date_str
    logger_scheduler.info(f"- Collecting Weather History for: {date_1_start} -")
    locations_list = db_session.query(Locations).all()
    weather_hist_call_counter = 0
    for location in locations_list:
        logger_scheduler.info(f"- Checking Weather History for: {location.id} - {location.city}, {location.country} -")
        weather_hist_exists = db_session.query(WeatherHistory).filter_by(
            location_id = location.id).filter_by(date_time=yesterday_date_str).first()
        
        if not weather_hist_exists:
            logger_scheduler.info(f"- Weather History on {date_1_start} does not exist for: {location.id}  -")
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
                add_weather_history(db_session, location.id, weather_data)
                logger_scheduler.info(f"- Successfully added Weather History for: location.id: {location.id} for date: {date_1_start} -")
    
    logger_scheduler.info(f"- Made  {weather_hist_call_counter} VC API calls for weather history locations ")
    wrap_up_session(db_session)

if __name__ == '__main__':  
    scheduler_initiator()