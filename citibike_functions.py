# Import libraries
import pandas as pd
import numpy as np
import re

# Function to process station data for a given year
def process_stationdata(year):
	"""This function will process citibke station data that was obtained from www.theopenbus.com/raw-data.
    The data set ranges from March 2015 to April 2019. The original datasets are tab-deliminated datasets with
    inconsistent column numbers and names. The data was preprocessed in the terminal command line by merging the
    datasets in a yearly level by running the following command: cat *.csv > merged.csv.
    The function processes each station data for a given year by slitting the tab-delimitors and making
    consistent column names. The script below then combines the data from all years into a single station.csv
    data file.
    """
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
	"""This function will clean the station data from that was merged in Linux command lines.
    The data cleaning function contains several new features including day of week, hour, and dock status.
    Dock status is calculated as available bikes / total docks. Running this function will ultimately provide
    a combined csv file called stations_cleaned.csv
    """
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


def cleaning_ridersdata(df):
    
    """This function will clean the riders data.
    The data cleaning function contains several new features.
    """
    
    df.starttime.astype('M8[us]')
    df.stoptime.astype('M8[us]')
    
    # Drop missing values
    df = df.dropna(subset=['start station id', 'end station id']).reset_index()
    
    # create year column
    df['year'] = df.starttime.dt.year
    print('completed year column')
    
    # create date column
    df['start_date'] = df.starttime.dt.date
    df['stop_date'] = df.stoptime.dt.date
    print('completed date column')
    
    # create hour column
    df['start_hour'] = df.starttime.dt.hour
    df['stop_hour'] = df.stoptime.dt.hour
    print('completed hour column')
    
    # create minutes column
    df['start_min'] = df.starttime.dt.minute
    df['stop_min'] = df.starttime.dt.minute
    print('completed minutes column')
    
    # For easier computation and analysis, we decided to round the minutes by 20 minutes time frame
    df['start_min'] = df['start_min'].apply(lambda x: '00' if x < 20 else '20' if x < 40 else '40')
    df['stop_min'] = df['stop_min'].apply(lambda x: '00' if x < 20 else '20' if x < 40 else '40')
    
    
    # create season column
    df['season'] = df.starttime.dt.month.apply(lambda x: 'winter' if x <= 2 else 'spring' if x <= 5 else \
    'summer' if x <= 8 else 'fall' if x <= 11 else 'winter')
    print('completed season column')
    
    # create day of week column
    df['dayofweek'] = df['starttime'].dt.weekday.apply(lambda x: 'Monday' if x == 0 else 'Tuesday' if x==1 else \
    'Wednesday'if x == 2 else 'Thursday' if x == 3 else 'Friday' if x == 4 else 'Saturday' if x == 5 else 'Sunday')
    print('completed week column')
    
    # create interval column
    df['start_interval'] = df.apply(lambda x: str(x['start_hour']) + ":" + str(x['start_min']), axis=1)
    df['stop_interval'] = df.apply(lambda x: str(x['stop_hour']) + ":" + str(x['stop_min']), axis=1)
