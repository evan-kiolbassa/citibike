# Citibike Machine Learning Project: Predicting the Rebalancing Needs for Citibike in New York City 

![1200px-Citi_Bike_logo svg](https://user-images.githubusercontent.com/29543481/102022669-5192f780-3d56-11eb-8066-709edd3275e6.png)

## Introduction

In May 2013, New York City launched Citibike, one of the largest bike-share system in United States. Today, Citibike in NYC consists of more than 1200 + active dock stations that has expanded to Brooklyn, and Jersey City. As of September 2020, despite of COVID-19, there are more than 40,000 trips every day with a total of 113M total trips taken since it was first launched. Despite CitiBike's growth, the service is not always satisfactory. For example, the massive use of the service by commuter results into the quick depletion stalls in the dock stations in the morning and the rapid exhaustion of the available stalls in the docking stations in the afternoon so that users cannot deposit more bikes. This disparity highlights the importance of finding efficient ways to manage the bike-sharing network in NYC. 

An effective way to enhance the CitBike system is relocating bikes from overcrowded stations to those with a shortage of bikes, which is known as rebalancing. There are multiple ways to do this: One, CitiBike can deploy a fleet of trucks to pick up and drop off bikes at different stations to balance the network. Two, Citibike can operate a bike train that carries 12-16 bikes to pick up and drop off bikes in narrow neighborhoods. Three, CitiBike can employ bike riders to move around bikes to improve the availability of bikes and docks. CitiBike is currently utilizing these methods (visit: https://www.citibikenyc.com/blog/rebalancing-the-citi-bike-system for more information) in order to solve bike/dock station availability issues. However, CitiBike is still being critized for its poor rebalancing strategy and management. 

In order to improve the current CitiBike's rebalancing strategy, this project will address two topics: Understanding and Analyzing CitiBike's Riders data and inventory status and predicting the demand for bikes and stalls in the docking stations in different lcations to relocate the bikes in order to best satisfy customer's demand.  

## Problem

![Screen Shot 2020-12-20 at 7 52 34 PM](https://user-images.githubusercontent.com/29543481/102729067-15830800-42fd-11eb-8728-112a160b9c98.png)

*Citibke has been constantly critized for its poor bike and dock station availability that can cause issues with customer retention*


Ever since Citibike was introduced, the bike share system has been constantly criticized by its cusotmer for its poor mehtod of stock redistribution in order to equalize the supply of bikes and stations. The massive use of bike-share system during commuting time results into quick depletion of the stations in residential areas in the morning and the rapid depletion of the stations in commercial areas in the afternoon. These unsatifactory feedbacks can be reflected on many of recent CitiBike-related tweets and articles. In addition, tourists disappointment can be reflected by the high rate of negative reviews of the service on TripAdvisor (for more information, visit: https://www.tripadvisor.com/Attraction_Review-g60763-d7071917-Reviews-Citi_Bike-New_York_City_New_York.html). 

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

![Screen Shot 2020-12-20 at 7 54 53 PM](https://user-images.githubusercontent.com/29543481/102729113-4ebb7800-42fd-11eb-85ca-15d0e08c8232.png)


## Data Preprocessing

1. Merging files: The Data Preprocessing folder and the data folder have all the necessary jupyter notebooks, python files, and bash scripts to merge and clean the riders data, station data, and the weather data indicated above. Because the data above are all saved separately either by a monthly level or yearly level, it is necessary to merge all the files into one csv files before cleaning the data. This step was done in Linux command lines using a bash script in order to merge all the files without the header.

2. Cleaning the data: All data cleaning-related functions can be found in the citibike_functions.py. However, in order to clean each data, please refer to each Jupyter Notebook to clean each data file. To clean the riders data, refer to *ridersdata_clean.ipynb* and to clean the dock station data, refer to *stationdata_cleaning.ipynb*

3. Downsampling


## Project Directory

- **Data Preprocessing**: contains all the jupyter notebooks and python files on preprocessing the data above
  - The Data Preprocessing folder and the data folder have all the necessary jupyter notebooks, python files, and bash scripts to merge and clean the riders data, station data, and the weather data indicated above. Because the data above are all saved separately either by a monthly level or yearly level, it is necessary to merge all the files into one csv files before cleaning the data. This step was done in Linux command lines using a bash script in order to merge all the files without the header.
  - merge files either merge the datasets inside the notebook or have functions that merge the datasets
  - clean files cleans the merged dataframes. Inside the jupyter notebook, you can find multiple functions that cleans the dataset
- **EDA**: Exploratory Data Analysis folder contains all the EDA files on riders data, dock station data, and weather data
  - EDA_Weather.ipynb analyzes the weather's impact on riders behavior and dock station status quo
  - EDA_dockstation.ipynb, EDA_dock_bk.ipynb, and Dock Stations Status EDA.ipynb analyzes the dock station
  - EDA.ipynb and EDA_rider_bk.ipynb analyzes CitiBike's riders behavior
  - Time Series of Dock Status.ipynb and Inventory Descriptive Time Series.ipynb analyzes Citibike's inventory and riders behavior in terms of time series
- **Models**
  - This project looks into several different machine learning models including K means Clustering, Random Forest Classification, Logistic Regression, and Time Series Analysis
  - Citibike Time Series.ipynb: Time Series Model
  - Distance.ipynb: finding all the possible *Manhattan* distance between all stations
  - Downsampling.ipynb and Downsampling_all_fields.ipynb: Jupyter notebook that downsamples some of the large datasets for the model
  - logistic_regression.bk.ipynb: Logistic Regression Model
  - model_clustering_hierarchy_kmeans_dock_stn_bk.ipynb and model_clustering_hierarchy_kmeans_rider_bk.ipynb: Clustering Model
  - random forest classifier.ipynb, random forest classifier2.ipynb - Random forest classification model
  - predictions.ipynb: brings all the random forest classification models to predict dock station's depletion status and incoming and outgoing bike demands
  - clf.pkl, clf_in.pkl, clf_out.pkl: Random forest classification models that are pickled. These files were not uploaded in the repository due its lage size 
  - 
- **data**
  - combined.csv: All Riders Data 
  - distance.csv (Haversine Distance)
  - distance2.csv (Manhattan Distance)
  - downsampled_rider.csv: Riders data downsampled
  - final_weather: merged and cleaned weather data
  - incoming.csv.gz: compressed incoming bike demand dataframe
  - outgoing.csv.gz: compressed outgoing bike demand dataframe
  - stations_cleaned.csv.gz: compressed merged and cleaned stations data
  - riders_cleaned.csv.gz: compressed merged and cleaned riders data
  - monthly_weather.csvp: monthly weather aggregated by month and day
- citibike_functions.py: containg all the functions used during data preprocessing

## Exploratory Data Analysis

Please refer to our EDA folder

## Machine Learning

Please refer to our Models folder



