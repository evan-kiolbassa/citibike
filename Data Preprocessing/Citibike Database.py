import psycopg2
import csv
import pandas as pd
import numpy as np
from psycopg2.extras import execute_batch

def unique_values(df):
    columns = pd.Series(df.columns)
    unique_df = pd.DataFrame()
    print('='*55)
    print('# of Unique Values in Each Column')
    for column in columns:
        unique_df[str(column)] = pd.Series(df.loc[:, column].unique())
        no_null = unique_df[str(column)].dropna()
        print('{}: {}'.format(column, len(no_null)))
    print('='*55)
    print('Max String Length for Object Type Columns')
    numeric = list(unique_df.describe().columns)
    for column in columns[~columns.isin(numeric)]:
        '''
        Dropping null values for each column in the unique dataframe
        and using pd.Series.apply to distribute length function on
        each value in the series and obtaining the max value to know
        length parameters for varchar data type
        '''
        no_null = unique_df[str(column)].dropna()
        str_length = no_null.apply(lambda x: len(x))
        max_length = str_length.max()
        print('{}: {}'.format(column, max_length))
    print('='*55)
    
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

         

def remove_numeric(series):
    '''
    A funtion that utilizes the pandas functionality of identifying 
    numeric strings. The tilde denotes that True values will be null.
    '''
    num_check = series.str.isnumeric()
    series = series.where(~num_check)
    return series
def df_to_sql():
    conn = psycopg2.connect('dbname = citibike user = postgres password = ****')
    cur = conn.cursor()
    cur.execute("""
             CREATE TABLE infrastructure.riders (
             tripduration INT8,
             starttime DATE,
             stoptime DATE,
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
    rider_iter = pd.read_csv(r'C:\Users\mmotd\OneDrive\Documents\Citibike\rider.csv', 
                            chunksize = 20000, encoding = 'utf-8', parse_dates= ['starttime', 'stoptime'], infer_datetime_format= True)
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
        lengths = chunk['birth year'].apply(lambda x: len(str(x)))
        chunk['birth year'] = chunk['birth year'].where(lengths == 4)
        chunk['birth year'] = chunk['birth year'].fillna(0)
        lengths = chunk['gender'].apply(lambda x: len(str(x)))
        chunk['gender'] = chunk['gender'].where(lengths == 1)
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

