import psycopg2
import csv
import pandas as pd
import numpy as np
import multiprocessing as mp
from psycopg2.extras import execute_batch
import os, zipfile
import glob
import random
from sqlalchemy import create_engine

def execute_query(query):
    '''
    A function that establishes a connection to the database,
    creates a cursor object, executes a query from the cursor object,
    commits the query, and closes the connection
    '''
    conn = psycopg2.connect('dbname = citibike user = postgres password = ***')
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    conn.close()
    print('Query Commited')


# Establishing server connection and creating crime_db
conn = psycopg2.connect("dbname = postgres user = postgres password = ***")
conn.autocommit = True
cur = conn.cursor()
cur.execute("""
            CREATE DATABASE citibike;
            """)
conn.autocommit = False
conn.close()

# Creating crimes schema for crime_db

execute_query("""
            CREATE SCHEMA infrastructure;
              """)

def remove_nonnumeric(series):
    '''
    A funtion that utilizes the pandas functionality of identifying 
    numeric strings to create boolean mask.
    '''
    num_check = series.str.isnumeric()
    series = series.where(num_check)
    return series
def remove_numeric(series):
    '''
    A funtion that utilizes the pandas functionality of identifying 
    numeric strings. The tilde denotes that True values will be null.
    '''
    num_check = series.str.isnumeric()
    series = series.where(~num_check)
    return series

def df_to_sql():
    conn = psycopg2.connect('dbname = citibike user = postgres password = ***')
    cur = conn.cursor()
    cur.execute("""
             CREATE TABLE infrastructure.riders (
             tripduration INT8,
             starttime TIMESTAMP,
             stoptime TIMESTAMP,
             start_station_id INT8,
             start_station_name VARCHAR(60),
             start_station_latitude FLOAT4,
             start_station_longitude FLOAT4,
             end_station_id INT8,
             end_station_name VARCHAR(60),
             end_station_latitude FLOAT4,
             end_station_longitude FLOAT4,
			 bikeid INT8,
			 usertype VARCHAR(10),
             birth_year SMALLINT,
             gender SMALLINT
             );
            """)
    '''
    Reads combined csv file of accumulated ride data in 20,000 interval chunks.
    Date time format is inferred due to inconsistency in formatting. 
    '''
    rider_iter = pd.read_csv(r'E:\Citibike\rider.csv', 
                            chunksize = 100000, encoding = 'utf-8', parse_dates= ['starttime', 'stoptime'], infer_datetime_format= True)
    numeric_columns = ['tripduration', 'start station id','start station latitude', 'start station longitude','end station id',
                    'end station latitude', 'end station longitude', 'bikeid', 'birth year', 'gender']
    for chunk in rider_iter:
        '''
        Data was collected from csv files for each month from 2013 to 2021. The files were combined using 
        the command line. Consequentially, there are lines where headers still exist. To remove these lines,
        the replace function is used to remove embedded header lines and make them null. Numeric lines are 
        coerced into numeric format making lines that are not able to be converted to numeric formats null.
        The birth year column is filled with instances of "\n" which denotes that the birth year was not input
        by the user, so these observations were filled with zero. Birth year and gender columns were also checked
        for lengths and made null if they were not within the length constraints. In the case of gender, zero was
        imputed (indicating unknown gender). 
        '''
        chunk = chunk.apply(lambda x: x.astype(str).replace(x.name,np.nan),axis = 1)
        chunk.loc[:, numeric_columns] = chunk.loc[:, numeric_columns].apply(lambda x: pd.to_numeric(x, errors = 'coerce'), axis = 1)
        chunk['birth year'] = chunk['birth year'].fillna(0)
        chunk['gender'] = chunk['gender'].fillna(0)
        chunk.columns = pd.Series(chunk.columns).str.replace(" ", "_")
        print('# of null values : {}'.format(chunk.isna().sum()))
        chunk.loc[:, ['starttime', 'stoptime']] = chunk.loc[:, ['starttime', 'stoptime']].apply\
            (lambda x: remove_numeric(x.astype(str)), axis = 1)
        station_null_index = chunk[chunk.end_station_id.isna()].index
        chunk.loc[station_null_index, ['end_station_id', 'end_station_latitude', 'end_station_longitude']] =\
            chunk.loc[station_null_index, ['start_station_id', 'start_station_latitude', 'start_station_longitude']]
        chunk['usertype'] = chunk['usertype'].fillna('Unknown')
        chunk = chunk.dropna()
        print(chunk.head())
        if len(chunk) > 0:
            '''
            Creates a list of column names for dataframe and prepares an insert statement using %s
            as value placeholders for the number of columns in each row. The INSERT statement is executed
            using the execute_batch command in the psycopg2 extras module
            '''
            df_columns = list(chunk)
            # create (col1,col2,...)
            columns = ", ".join(df_columns)

            # create VALUES('%s', '%s",...) one '%s' per column
            values = "VALUES({})".format(", ".join(["%s" for _ in df_columns])) 

            #create INSERT INTO table (columns) VALUES('%s',...)
            insert_stmt = "INSERT INTO infrastructure.riders ({}) {}".format(columns,values)
            psycopg2.extras.execute_batch(cur, insert_stmt, chunk.values)
    conn.commit()
    cur.close()    
    print('SQL Insert Complete')
df_to_sql()
   
# Creating user groups and granting necessary permissions
execute_query("""
            REVOKE ALL ON SCHEMA public FROM public;
            REVOKE ALL ON DATABASE citbike FROM public;
            CREATE GROUP readonly NOLOGIN;
            CREATE GROUP readwrite NOLOGIN;
            GRANT CONNECT ON DATABASE citibike TO readonly;
            GRANT CONNECT ON DATABASE citibike TO readwrite;
            GRANT USAGE ON SCHEMA infrastructure TO readonly;
            GRANT USAGE ON SCHEMA infrastructure TO readwrite;
            GRANT SELECT ON ALL TABLES IN SCHEMA infrastructure 
            TO readonly;
            GRANT INSERT, SELECT, DELETE, UPDATE ON ALL TABLES 
            IN SCHEMA infrastructure TO readwrite;
              """)



alchemyEngine = create_engine('postgresql+psycopg2://postgres:***@localhost:5432/citibike')

dbConnection = alchemyEngine.connect()

# Read data from PostgreSQL database table and load into a DataFrame instance

pd.read_sql("SELECT start_station_name, end_station_name FROM infrastructure.riders LIMIT 200", dbConnection)

dir_name = r'C:\Users\mmotd\OneDrive\Documents\Citibike\Inventory'
extension = ".zip"

os.chdir(dir_name) # change directory from working dir to dir with files

for item in os.listdir(dir_name): # loop through items in dir
    if item.endswith(extension): # check for ".zip" extension
        file_name = os.path.abspath(item) # get full path of files
        zip_ref = zipfile.ZipFile(file_name) # create zipfile object
        zip_ref.extractall(dir_name) # extract file to dir
        zip_ref.close() # close file
        os.remove(file_name) # delete zipped file


os.mkdir(r'C:\Users\mmotd\OneDrive\Documents\Citibike\Stations')
for subdir, dirs, files in os.walk(dir_name):
    for file in files:
        if file == 'bikeshare_nyc_raw.csv':
            full_path = os.path.join(subdir, file)
            os.rename(full_path, r"C:\Users\mmotd\OneDrive\Documents\Citibike\Stations\{}{}".format(random.randint(1000,3000), file))

os.chdir(r'C:\Users\mmotd\OneDrive\Documents\Citibike\Stations')

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
#combine all files in the list
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
#export to csv
combined_csv.to_csv( r"C:\Users\mmotd\OneDrive\Documents\Citibike\Stations\inventory.csv", index=False) # Combining csv from directory into singular csv file

def inventory_process():
    
    execute_query(""" 
        CREATE TABLE infrastructure.inventory (
            dock_id TEXT, 
            dock_name TEXT, 
            date TEXT, 
            hour TEXT, 
            minute TEXT, 
            pm TEXT, 
            avail_bikes TEXT, 
            avail_docks TEXT, 
            tot_docks TEXT, 
            _lat TEXT, 
            _long TEXT, 
            in_service TEXT, 
            status_key TEXT
        );            
        """)
    conn = psycopg2.connect('dbname = citibike user = postgres password = ***')
    cur = conn.cursor()
    inventory = pd.read_csv(r"E:\Citibike\Stations\inventory.csv",
                        error_bad_lines = False, warn_bad_lines= True, chunksize = 100000)

    for chunk in inventory:
        # Split dataframe into multiple columns and save that to a new temporary dataframe
        # Generate the list of columns to be used
        # Split dataframe into multiple columns and save that to a new temporary dataframe
        chunk = chunk.iloc[:,0].astype(str).str.split("\t", expand = True)
        print(chunk)
        if len(chunk.columns) == 13:
                chunk.columns = ['dock_id', 'dock_name', 'date', 'hour', 'minute', 'pm', 'avail_bikes', 'avail_docks', 'tot_docks', '_lat', '_long', 'in_service', 'status_key']
                '''
                Creates a list of column names for dataframe and prepares an insert statement using %s
                as value placeholders for the number of columns in each row. The INSERT statement is executed
                using the execute_batch command in the psycopg2 extras module
                '''
                df_columns = chunk.columns.astype(str)
                columns = ", ".join(df_columns)
                print(columns)
                # create VALUES('%s', '%s",...) one '%s' per column
                values = "VALUES({})".format(", ".join(["%s" for _ in df_columns]))
                print(values)
                #create INSERT INTO table (columns) VALUES('%s',...)
                insert_stmt = "INSERT INTO infrastructure.inventory ({}) {}".format(columns,values)
                psycopg2.extras.execute_batch(cur, insert_stmt, chunk.values)
    conn.commit()
    cur.close()    
    print('SQL Insert Complete')

inventory_process()

execute_query("""
            CREATE SCHEMA infrastructure_final;

            CREATE TABLE infrastructure_final.Ride_Data 
                        (
                        tripduration INT8,
                        starttime TIMESTAMP,
                        stoptime TIMESTAMP,
                        start_station_id INT8,
                        end_station_id INT8,
                        bikeid INT8,
                        usertype VARCHAR(10),
                        birth_year SMALLINT,
                        gender SMALLINT
                        );

            CREATE TABLE infrastructure_final.Stations 
                        (
                        station_name VARCHAR(60),
                        station_id INT8,
                        lat FLOAT4,
                        lon FLOAT4
                        );

            CREATE TABLE infrastructure_final.Inventory 
                         (
                        dock_id INT8,  
                        date TIMESTAMP, 
                        avail_bikes INT4, 
                        avail_docks INT4, 
                        tot_docks INT4, 
                        in_service SMALLINT, 
                        status_key SMALLINT
                         );
              """)


execute_query("""
            INSERT INTO infrastructure_final.Ride_Data
            SELECT tripduration,
            starttime,
            stoptime,
            start_station_id,
            end_station_id,
            bikeid,
            usertype,
            birth_year,
            gender 
            FROM infrastructure.riders
            """)

execute_query("""
            INSERT INTO infrastructure_final.Stations
                    SELECT 
                        start_station_name AS station_name,
                        start_station_id AS station_id,
                        AVG(start_station_latitude) AS lat,
                        AVG(start_station_longitude) AS lon
                    FROM infrastructure.riders
                    GROUP BY start_station_id, start_station_name
            """)
date_df = pd.read_sql("""
                        SELECT 
                            dock_id, 
                            date, 
                            hour, 
                            minute, 
                            pm, 
                            avail_bikes, 
                            avail_docks, 
                            tot_docks, 
                            in_service, 
                            status_key
                        FROM infrastructure.inventory 
                          """, dbConnection, chunksize = 1000000)
for chunk in date_df:
    chunk.apply(lambda x: print(x.unique()))

def inventory_clean():
    conn = psycopg2.connect('dbname = citibike user = postgres password = ***')
    cur = conn.cursor()
    date_df = pd.read_sql("""
                        SELECT 
                            dock_id, 
                            date, 
                            hour, 
                            minute, 
                            pm, 
                            avail_bikes, 
                            avail_docks, 
                            tot_docks, 
                            in_service, 
                            status_key
                        FROM infrastructure.inventory 
                          """, dbConnection, chunksize = 1000000)
    for chunk in date_df:

        '''
        Combining the separated date, hour, minute, and pm boolean columns into a single datetime object
        '''
        chunk.date = chunk.date.str.replace('"', '')
        chunk['suffix'] = ['PM'if i == '1' else 'AM' for i in chunk.pm] # Conversion of boolean statement to AM or PM string
        chunk.loc[:,['dock_id','avail_bikes', 'avail_docks', 'tot_docks', 'in_service', 'status_key']] =\
        chunk.loc[:,['dock_id','avail_bikes', 'avail_docks', 'tot_docks', 'in_service', 'status_key']].apply(lambda x: x.replace('None', '0', regex = True), axis = 1)
        chunk.loc[:,['dock_id','avail_bikes', 'avail_docks', 'tot_docks', 'in_service', 'status_key']] =\
        chunk.loc[:,['dock_id','avail_bikes', 'avail_docks', 'tot_docks', 'in_service', 'status_key']].apply(lambda x: x.replace('\D', '', regex = True), axis = 1)
        chunk = chunk.dropna() # There are large chunks of null values as a result of the csv concatenation
        chunk.loc[:,['avail_bikes', 'avail_docks', 'tot_docks']] =\
        chunk.loc[:,['avail_bikes', 'avail_docks', 'tot_docks']].apply(lambda x: [0 if len(i) == 0 else i for i in x]) # Removes empty strings
        chunk['date'] = ['20' + i for i in chunk.date] # Inserting 20 in front of the date column
        chunk['minute'] = ['0' + i if len(i) == 1 else i for i in chunk.minute] # Inserting a 0 in front of value if length equal to 1
        chunk['date'] = chunk.date + ' ' + chunk.hour + ':' + chunk.minute + ' ' + chunk.suffix # Combining columns to form date string
        chunk['date'] = pd.to_datetime(chunk.date, yearfirst=True) # Conversion to datetime object
        chunk = chunk[chunk.dock_id != '']
        chunk = chunk[chunk.status_key != '']
        final_df = chunk.loc[:, ['dock_id', 'date', 'avail_bikes', 'avail_docks', 'tot_docks', 'in_service', 'status_key']]
        print(final_df.head())
        
        '''
        Creates a list of column names for dataframe and prepares an insert statement using %s
        as value placeholders for the number of columns in each row. The INSERT statement is executed
        using the execute_batch command in the psycopg2 extras module
        '''
        df_columns = final_df.columns
        columns = ", ".join(df_columns)
        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(", ".join(["%s" for _ in df_columns]))
        print(values)
        #create INSERT INTO table (columns) VALUES('%s',...)
        insert_stmt = "INSERT INTO infrastructure_final.Inventory ({}) {}".format(columns,values)
        psycopg2.extras.execute_batch(cur, insert_stmt, final_df.values)
    conn.commit()
    cur.close()    
    print('SQL Insert Complete')
inventory_clean()

pd.read_sql(""" SELECT COUNT(date) FROM infrastructure_final.Inventory """, dbConnection)