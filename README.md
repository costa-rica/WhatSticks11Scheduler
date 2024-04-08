# What Sticks 11 Scheduler

![What Sticks Logo](https://what-sticks.com/website_images/wsLogo180noName.png)

## Description
This app is designed to collect weather daily for the What Sticks 11 Platform. This app calls Visual Crossing for daily city level weather data.

## How it works

Set up when the app should collect the data inside the `scheduler_initiator()` function of the scheduler.py file.

### Run from terminal
#### Step 1: 
Activate venv with ws_models, ws_config, and ws_utilities - amongst other necessary packages.
#### Step 2:
navigate to WhatSticks11Scheduler project folder with scheduler.py and run:
```
python scheduler.py
```
