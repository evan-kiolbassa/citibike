# Import libraries
import pandas as pd
import numpy as np
import re


import pandas as pd

# Function to process station data for a given year
def process_stationdata(year):
    df = pd.read_csv(f"../data/stationdata/merged{year}.csv")

    # Generate the list of columns to be used
    colnames = df.columns[0].split('\t')
    df.columns = ['_']

    
    # Split dataframe into multiple columns and save that to a new temporary dataframe
    df_expand = df['_'].str.split("\t", expand = True)

    
    # Drop any unnecessary columns created by the splitting
    if df_expand.shape[1]>13:
        df_expand.drop(list(range(13, df_expand.shape[1])), axis = 1, inplace = True)
    
    
    # Assign the correct column names to the dataframe and write it to disk
    df_expand.columns = colnames[0:13]
    
    
    # Save as new csv file
    df_expand.to_csv(f"../data/stationdata/stations{year}.csv", index = False)    







def cleaning_stationdata(df):
    df.dropna(inplace = True)
    df.drop(df[df['dock_id'].apply(lambda x: isinstance(x, str))].index, inplace = True)
    df = df[df['tot_docks'] < 500]
    
    
    mask = ~df['avail_bikes'].astype(str).str.contains('[A-z]')
    df = df[mask]
    
    
    # Remove quotation marks
    df['avail_bikes'] = df['avail_bikes'].apply(lambda x: re.sub("\"", "", str(x)))
    # Drop empty values
    df = df[df['avail_bikes'] != ""]
    # Convert strings to integers
    df['avail_bikes'] = df['avail_bikes'].astype(float).astype(int)
    # Remove any row with an impossible number of bikes
    df = df[df['avail_bikes'] <= 200]
    
    mask = ~df['avail_docks'].astype(str).str.contains('[A-z]')
    df = df[mask]

    
    # Remove quotation marks 
    df['avail_docks'] = df['avail_docks'].apply(lambda x: re.sub("\"", "", str(x)))
    # Drop empty values
    df = df[df['avail_docks'] != ""]
    # Convert strings to integers
    df['avail_docks'] = df['avail_docks'].astype(float).astype(int)
    # Remove number of available docks that are higher than 200
    df = df[df['avail_docks'] <= 200]
    
    
    # Parse date column into datetime format
    df['date'] = pd.to_datetime(df['date'], format = '"%y-%m-%d"')

    
    # Convert numeric columns from strings to integers
    df['dock_id'] = df['dock_id'].astype(int)
    df['tot_docks'] = df['tot_docks'].astype(int)
    df['minute'] = df['minute'].astype(int)
    
    # Clean up latitude and longitude columns
    df['_lat'] = df['_lat'].apply(lambda x: float(re.sub('\"', "", str(x))))
    df['_long'] = df['_long'].apply(lambda x: re.sub('[^-^.0-9]', "", str(x))).apply(lambda x: re.sub("-{2}", "-", str(x)))
    df = df[df['_long'] != ""]
    df['_long'].astype(float)
    
    
    # Clean up hours column
    df['hour'] = df['hour'].apply(lambda x: re.sub('[^0-9]', "", str(x))).astype(int)
    
    # Convert hours to 24-hour time
    df['hour'].loc[df['pm'] == 1] = df['hour'].loc[df['pm'] == 1] + 12

    # Remove quotations from dock name
    df['dock_name'] = df['dock_name'].apply(lambda x: str(re.sub('\"', "", x)))


    # Create a depletion status column
    df['depletion_status'] = (df['avail_bikes']/df['tot_docks']).apply(lambda x: "Full Risk" if x > 2/3 else "Empty Risk" if x < 1/3 else "Healthy")


    # Drop unnecessary columns
    df.drop(['pm'], axis = 1, inplace = True)


    # Create new variables -> time, day of the week, and season
    # time variable
    df = df.assign(time = lambda x: x['hour'].astype(str) + ":" + x['minute'].astype(str))
    # day of the week variable
    df = df.assign(dayofweek = lambda x: x['date'].dt.weekday)
    # season variable
    df = df.assign(\
        season = lambda x: x['date'].dt.month.apply(\
        lambda y: 'winter' if y <= 2 else 'spring' if y <= 5 else 'summer' if y <= 8 else 'fall' if y <= 11 else 'winter'))
    
    # Create a new csv file
    df.to_csv("../data/stations_cleaned.csv", index = False)




def clean_weatherdata(df):
    """This function will clean the weather data from any given year or years (the merged weather data)
    Weather data was obtained from NOAA( National Cceanic and Atmospheric Administration ) 
    https://www.ncei.noaa.gov/data/global-hourly/archive/csv/.
    Additional websites were also used as references in order to interpret the numbers from the data and to
    engineer new features such as windy and rainy. The column windy and its values (breeze, gale, force...)
    were all based on information from NOAA https://www.weather.gov/pqr/wind. 
    https://www.visualcrossing.com/resources/documentation/weather-data/how-we-process-integrated-surface-database-historical-weather-data/
    was used to interpret the numbers of the data.
    """
    # Make a copy
    nyweather= df.copy()
    
    # Select the columns that will be used
    nyweather = nyweather[['DATE', 'SOURCE', 'LATITUDE', 'LONGITUDE', 'ELEVATION', 'NAME', 'WND', 'TMP',
             'DEW', 'SLP', 'AA1','AA2']]
    
    # Clean DATE column
    nyweather['DATE'] = nyweather['DATE'].str.replace('T', ' ')
    
    # Clean TMP (temperature) column. Please refer to 
    # Interpreting the numbers are based on 
    #https://www.visualcrossing.com/resources/documentation/weather-data/how-we-process-integrated-surface-database-historical-weather-data/

    nyweather['TMP'] = nyweather['TMP'].astype(str)
    nyweather['TMP'] = nyweather['TMP'].str.slice(0, -2)
    nyweather['TMP'] = nyweather['TMP'].str.replace('+','')
    nyweather['TMP'] = nyweather['TMP'].astype(int)
    nyweather['TMP'] = nyweather['TMP']/10
    
    print('complete part 1')
    
    # Clean WND column
    # Please refer to https://www.weather.gov/pqr/wind
    nyweather['WND'] = nyweather['WND'].astype(str)
    nyweather = pd.concat([nyweather, nyweather.WND.str.split(',', expand = True)],1)
    nyweather = nyweather.rename(columns={3:'wind_speed'})
    nyweather['wind_speed'] = nyweather['wind_speed'].astype(int)
    nyweather['wind_speed'] = nyweather['wind_speed']/10
    nyweather['wind_speed'] = nyweather['wind_speed'].map(to_milesperhour)
    
    # Engineered a new feature called windy
    nyweather['windy'] = nyweather['wind_speed'].apply(lambda x: 'calm' if x < 4 else 'breeze' if x < 12 else \
                                        'moderate breeze' if x < 24 else 'strong breeze' if x < 31 else \
                                        'gale' if x < 63 else 'storm force')
    nyweather = nyweather[['DATE', 'SOURCE', 'LATITUDE', 'LONGITUDE', 'ELEVATION', 'NAME', 'WND', 'TMP',\
                           'DEW', 'SLP', 'AA1','AA2', 'wind_speed', 'windy']]
    
    print('complete part 2')
    
    # Clean the AA1 column that has precipitation information
    nyweather['AA1'] = nyweather['AA1'].fillna('0,0,0,0')
    nyweather['AA1'] = nyweather['AA1'].astype(str)
    nyweather = pd.concat([nyweather, nyweather.AA1.str.split(',', expand = True)],1)
    nyweather = nyweather.rename(columns={1:'precipitation'})
    nyweather['precipitation'] = nyweather['precipitation'].astype(int)
    nyweather['precipitation'] = nyweather['precipitation']/10
    nyweather = nyweather[['DATE', 'SOURCE', 'LATITUDE', 'LONGITUDE', 'ELEVATION', 'NAME', 'WND', 'TMP',\
                           'DEW', 'SLP', 'wind_speed', 'windy', 'precipitation']]
    
    # Engineered a new feature called rainy
    nyweather['rainy'] = nyweather['precipitation'].apply(lambda x: 'rainy' if x > 0 else 'not rainy')
    
    
    # Clean DATE column
    # Generate new features - month, date, hour and month2
    nyweather['DATE'] = nyweather['DATE'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    nyweather['month'] = nyweather.DATE.dt.month
    nyweather['date'] = nyweather.DATE.dt.date
    nyweather['hour'] = nyweather.DATE.dt.hour
    nyweather['month2'] = nyweather.DATE.dt.month.apply(lambda x: 'Jan' if x == 1 else 'Feb' if x == 2 else \
    'Mar' if x == 3 else 'Apr' if x == 4 else 'May' if x == 5 else 'Jun' if x == 6 else 'Jul' if x == 7 else \
    'Aug' if x == 8 else 'Sep' if x == 9 else 'Oct' if x == 10 else 'Nov' if x == 11 else 'Dec' )
    
    # Generate cleaned dataframe
    return nyweather




def to_milesperhour(num):
    """A function to convert wind speed from meters/second to miles/hour. The conversion is done by using the 
    above formula
    """
    convert = (num * 3600)/(1000 * 1.6)
    return convert

