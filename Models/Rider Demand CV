import pandas as pd
import pickle
import seaborn as sns
import sklearn
import matplotlib.pyplot as plt
import numpy as np
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessMonthBegin
import datetime
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.model_selection import train_test_split
from pprint import pprint
plt.style.use('fivethirtyeight')

def feature_engineering(df):
    df['dayofweek'] = df.index.get_level_values(0).dayofweek
    df['weekend'] = [1 if (i == 6) | (i == 7) else 0 for i in df['dayofweek']]
    df['start_date'] = df.index.get_level_values(0).date
    df['time'] = df.index.get_level_values(0).time
    df['hour'] = df.index.get_level_values(0).hour
    
    federal_holidays = CustomBusinessMonthBegin(calendar=USFederalHolidayCalendar())
    holidays = pd.date_range(start='01/01/2013', end='11/30/2020',freq=federal_holidays).tolist()
    holidays = [i.date() for i in holidays]
    df['holidays'] = df['start_date'].isin(holidays).astype(int)
    
    df['month'] = df.index.get_level_values(0).strftime('%m')
    
    df['quarter'] = [1 if (i >= '01' and i <= '03') else 2 if (i >= '04' and i <= '06') else 3\
                                     if (i >= '07' and i <= '09') else 4 for i in df['month']]
    '''
    Creating a dataframe that distinguishes between winter and summer daylight savings times.
    A 0 denotes winter while a 1 denotes summer. Filtered the winter dsv and summer dsv and stored
    into separate lists. Created boolean columns for each category and created a column that determines
    if the timestamp does not fall under the dsv boundaries.
    '''
    
    sorted_times = pd.Series(df.index.get_level_values(0)).sort_values(ascending = False)
    start = sorted_times.min()
    end = sorted_times.max()
    dates = pd.date_range(start=start, end=end,  tz='US/Eastern')
    df1 = pd.DataFrame({'dst_flag': 1, 'date1': dates.tz_localize(None)}, index=dates)

    # add extra day on each end so that there are no nan's after the join    
    dates = pd.to_datetime(pd.date_range(start=start - pd.to_timedelta(1, 'd'), end=end + pd.to_timedelta(1, 'd'), freq='h'), utc=True)
    df2 = pd.DataFrame({'date2': dates.tz_localize(None)}, index=dates)
    
    out = df1.join(df2)
    out['dst_flag'] = (out['date1'] - out['date2']) / pd.to_timedelta(1, unit='h') + 5
    out.drop(columns=['date1', 'date2'], inplace=True)
    summer_dst = pd.Series(out[out['dst_flag'] == 1].index).dt.strftime('%Y-%m-%d')
    winter_dst = pd.Series(out[out['dst_flag'] == 0].index).dt.strftime('%Y-%m-%d')
    total_dst = summer_dst + winter_dst
    df['summer_dst'] = df['start_date'].astype(str).isin(summer_dst.astype(str)).astype(int)
    df['winter_dst'] = df['start_date'].astype(str).isin(winter_dst.astype(str)).astype(int)
    df['not_dst'] = df['start_date'].astype(str).isin(total_dst.astype(str)).astype(int)
    # Reversing the boolean values. Want 0 to be equal to dsv and 1 to be not dsv
    df['not_dst'] = [0 if i == 1 else 1 for i in df['not_dst']]
    
    df['dayofyear'] = df.index.get_level_values(0).dayofyear
    df['year'] = df.index.get_level_values(0).year
    df['season'] = df.index.get_level_values(0).month
    df['season'] = df['season'].apply(lambda x: 1 if x <= 2 else 2 if x <= 5 else \
    3 if x <= 8 else 4 if x <= 11 else 1)
    print('completed season column')
    df = df.drop(['start_date', 'time'],axis = 1)
    return df

def time_agg_sum(df,variable, frequency, groupby, group_var):
    if groupby == False:
        agg_df = pd.DataFrame(df.groupby([pd.Grouper(freq=frequency)])[variable].sum().reset_index())
        agg_df = feature_engineering(agg_df)
    else: 
        agg_df = pd.DataFrame(df.groupby([pd.Grouper(freq=frequency), group_var])[variable].count().reset_index())
        agg_df = feature_engineering(agg_df)
    return agg_df

def time_agg_mean(df,variable, frequency, groupby, group_var):
    if groupby == False:
        agg_df = pd.DataFrame(df.groupby([pd.Grouper(freq=frequency)])[variable].mean().reset_index())
        agg_df = feature_engineering(agg_df)
    else: 
        agg_df = pd.DataFrame(df.groupby([pd.Grouper(freq=frequency), group_var])[variable].mean().reset_index())
        agg_df = feature_engineering(agg_df)
    return agg_df

def model_input():
    rider_df = pd.read_csv('model_data.csv.gz', 
                      parse_dates = ['starttime', 'stoptime', 'start_date', 'stop_date'])
    station_data = pd.read_csv('stations_cleaned.csv.gz')
    rider_df = rider_df.set_index(rider_df['starttime'])
    rider_df = rider_df.groupby([pd.Grouper(freq = 'H'), 'start station id']).agg({'start station latitude': \
                                                                        'mean', 'start station longitude' : 
                                                                        'mean', \
                                                                        'start station id' : 'count',
                                                                        'start station name' : lambda x:x.value_counts().index[0]})
    rider_df = feature_engineering(rider_df)
    rider_df = rider_df.rename(columns = {'start station id' : 'rider_demand'})
    features = rider_df.loc[:,['start station latitude', 'start station longitude', 'start station name', 
    'dayofweek', 'weekend', 'hour', 'holidays',
       'month', 'quarter', 'summer_dst', 'winter_dst', 'not_dst', 'dayofyear',
       'year', 'season']]
    features.index = rider_df.index.rename(['datetime', 'dock_id'])
    target = rider_df['rider_demand']
    station_data.hour = station_data.hour.astype(str).apply(lambda x: x.zfill(2))
    station_data.loc[station_data['hour'] == '24', 'hour'] = '00'
    station_data.minute = station_data.minute.astype(str).apply(lambda x: x.zfill(2))
    station_data['time'] = station_data.hour + ':' + station_data.minute + ':' +  '00'
    station_data['datetime'] = pd.to_datetime(station_data['date'] + ' ' + station_data['time'])
    station_data = station_data.set_index(station_data['datetime'])
    station_data = station_data.groupby([pd.Grouper(freq = 'H'), 'dock_id'])['avail_bikes'].mean()
    station_data = station_data.interpolate(method='linear')
    station_data = station_data[station_data != 0]
    features = features.merge(station_data, left_index = True, right_on = ['datetime', 'dock_id'])
    features = features.drop(['avail_bikes'], axis = 1)
    print(features.shape)
    target = target[features.index]
    X_train, X_test, y_train, y_test = train_test_split(features, target, train_size=0.7, random_state=1)
    return X_train.drop(['weekend', 'holidays', \
                      'month', 'quarter', 'summer_dst', 'winter_dst', \
                      'not_dst', 'season' ], axis = 1), X_test.drop(['weekend', 'holidays', \
                      'month', 'quarter', 'summer_dst', 'winter_dst', \
                      'not_dst', 'season' ], axis = 1), y_train, y_test

def evaluate_model(model, x_train, y_train, x_test, y_test):
    import time
    n_trees = model.get_params()['n_estimators']
    n_features = x_train.shape[1]
    
    # Train and predict 10 times to evaluate time and accuracy
    predictions = []
    run_times = []
    for _ in range(5):
        print(_)
        start_time = time.time()
        model.fit(x_train, y_train)
        predictions.append(model.predict(x_test))
    
        end_time = time.time()
        run_times.append(end_time - start_time)
    
    # Run time and predictions need to be averaged
    run_time = np.mean(run_times)
    predictions = np.mean(np.array(predictions), axis = 0)
    
    # Calculate performance metrics
    errors = abs(predictions - y_test)
    mean_error = np.mean(errors)
    mape = 100 * np.mean(errors / y_test)
    accuracy = 100 - mape
    
    # Return results in a dictionary
    results = {'time': run_time, 'error': mean_error, 'accuracy': accuracy, 'n_trees': n_trees, 'n_features': n_features}
    
    return results

def run_model(X_train, X_test, y_train, y_test):
    # Number of trees in random forest
    n_estimators = [int(x) for x in np.linspace(start = 200, stop = 2000, num = 10)]
    # Number of features to consider at every split
    max_features = ['auto', 'sqrt']
    # Maximum number of levels in tree
    max_depth = [int(x) for x in np.linspace(10, 110, num = 11)]
    max_depth.append(None)
    # Minimum number of samples required to split a node
    min_samples_split = [2, 5, 10]
    # Minimum number of samples required at each leaf node
    min_samples_leaf = [1, 2, 4]
    # Method of selecting samples for training each tree
    bootstrap = [True, False]
    # Create the random grid
    random_grid = {'n_estimators': n_estimators,
                   'max_features': max_features,
                   'max_depth': max_depth,
                   'min_samples_split': min_samples_split,
                   'min_samples_leaf': min_samples_leaf,
                   'bootstrap': bootstrap}
    base_model = RandomForestRegressor()
    # Random search of parameters, using 3 fold cross validation, 
    # search across 100 different combinations, and use all available cores
    base_random = RandomizedSearchCV(estimator = base_model, 
                                   param_distributions = random_grid, 
                                   n_iter = 100, cv = 3, verbose=2, random_state=42, n_jobs = 32)
    base_random.fit(X_train, y_train)
    best_grid = base_random.best_estimator_
    def evaluate(model, test_features, test_labels):
        predictions = model.predict(test_features)
        errors = abs(predictions - test_labels)
        mape = 100 * np.mean(errors / test_labels)
        accuracy = 100 - mape
        print('Model Performance')
        print('Average Error: {:0.4f} degrees.'.format(np.mean(errors)))
        print('Accuracy = {:0.2f}%.'.format(accuracy))
    
        return accuracy
    grid_accuracy = evaluate(best_grid, X_test, y_test)
    return base_random.best_score_, base_random.best_params_,base_random.best_estimator_, \
    grid_accuracy, base_random.best_estimator_.feature_importances_feature_
    
X_train, X_test, y_train, y_test = model_input()
print(X_train.shape)
best_score, best_params, best_estimator, accuracy, feature_importance = run_model(X_train, X_test, y_train, y_test)

def model_build(X_train, X_test, y_train, y_test, best_params):
    model = RandomForestRegressor(best_params)
    model.fit(X_train, y_train)
    results = evaluate_model(model, X_train, y_train, X_test, y_test)
    return model, results


ride_demand_model, results = model_build(X_train, X_test, y_train, y_test, best_params)