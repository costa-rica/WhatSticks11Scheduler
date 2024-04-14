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
from ws_utilities import collect_yesterday_weather_history_from_visual_crossing
    # add_weather_history, request_visual_crossing_for_one_day
    


def scheduler_initiator():
    logger_scheduler.info(f'--- Started What Sticks 11 Scheduler ---')

    scheduler = BackgroundScheduler()

    job_ws_weather_and_UserLocationDay_updater = scheduler.add_job(scheduler_manager, 'cron', day='*', hour='01', minute='00', second='00')#Production
    # job_ws_weather_and_UserLocationDay_updater = scheduler.add_job(scheduler_manager, 'cron', hour='*', minute='47', second='25')#Testing
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

    # logger_scheduler.info("Start Interpolation")
    # interpolate_UserLocationDay_manager()

    logger_scheduler.info("Start Send Visual Crossing API weather reports")
    update_weather_history()
    logger_scheduler.info("--- ENDED What Sticks 11 Scheduler ---")

# # OBE due to ws_analysis/ create_df_daily_user_location_consecutive
# def interpolate_UserLocationDay_manager():
#     db_session = DatabaseSession()
#     users_list = db_session.query(Users).all()

#     for user in users_list:
#         query = db_session.query(UserLocationDay).filter(UserLocationDay.user_id == user.id)
#         # # Convert the query result to a list of dictionaries
#         df_existing_user_locations = pd.read_sql(query.statement, db_session.bind)

#         if len(df_existing_user_locations) > 1:
#             interpolated_df = interpolate_missing_dates_exclude_references(df_existing_user_locations)

#             logger_scheduler.info(f"interpolated_df Length: {len(interpolated_df)}")

#             if len(interpolated_df) > 0:
#                 interpolated_df.to_sql('user_location_day', con=engine, if_exists='append', index=False)
    
#     wrap_up_session(db_session)


def update_weather_history():
    collect_yesterday_weather_history_from_visual_crossing()
    # ###########################################################################################
    # # This function updates the weather history for yesterday for all locations in Locations ##
    # ###########################################################################################  

    logger_scheduler.info(f"- Finished update_weather_history from WS11Scheduler ")

if __name__ == '__main__':  
    scheduler_initiator()