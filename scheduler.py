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

    logger_scheduler.info("Start Send Visual Crossing API weather reports")
    collect_yesterday_weather_history_from_visual_crossing()
    logger_scheduler.info("--- ENDED What Sticks 11 Scheduler ---")


if __name__ == '__main__':  
    scheduler_initiator()