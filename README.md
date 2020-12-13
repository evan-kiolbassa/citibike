# Citibike Machine Learning Project: Predicting the Rebalancing Needs for Citibike in New York City 

![1200px-Citi_Bike_logo svg](https://user-images.githubusercontent.com/29543481/102022669-5192f780-3d56-11eb-8066-709edd3275e6.png)

## Introduction

This part is the introduction

## Problem

Citibke has been constantly critized for its poor bike and dock station availability that can cause issues with customer retention

![Screen Shot 2020-12-13 at 3 03 41 PM](https://user-images.githubusercontent.com/29543481/102022399-9d44a180-3d54-11eb-90d3-68fd92e8dc46.png)

Ever since Citibike was introduced, the bike share system has been constantly criticized by its cusotmer for it sppor mehtod of stock redistribution in order to equalize the supply of bikes and stations. 

## Objective

This project will:
- Analyze Citibike's current usage in NYC
- Evaluate Citibike's rebalancing strategy
- Develop a new rebalancing strategy using exploratory analysis and machine learning models

![Screen Shot 2020-12-13 at 3 14 46 PM](https://user-images.githubusercontent.com/29543481/102022677-5c4d8c80-3d56-11eb-9ac2-9afaa6b511e9.png)

## About the Data

1. **Riders Data**: The anonymous trip system data found at: https://www.citibikenyc.com/system-data
2. **Station Data**: Dock-station bike stocking information from the google-drive of: https://www.theopenbus.com/raw-data.html
3. **Weather Data**: Local hourly weather information from **noaa** at: https://www.ncei.noaa.gov/data/global-hourly/archive/csv/


## Data Preprocessing

1. Merging files: The Data Preprocessing folder and the data folder have all the necessary jupyter notebooks, python files, and bash scripts to merge and clean the riders data, station data, and the weather data indicated above. Because the data above are all saved separately either by a monthly level or yearly level, it is necessary to merge all the files into one csv files before cleaning the data. This step was done in Linux command lines using a bash script in order to merge all the files without the header.

2. Cleaningi the data: All data cleaning-related functions can be found in the citibike_functions.py. However, in order to clean each data, please refer to each Jupyter Notebook to clean each data file. To clean the riders data, refer to *ridersdata_clean.ipynb* and to clean the dock station data, refer to *stationdata_cleaning.ipynb*


## Exploratory Data Analysis



