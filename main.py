import glob
import pandas as pd
import os

import datetime as dt
import numpy as np

import matplotlib.pyplot as plt
import matplotlib

import plotly.graph_objects as go

from helpers import get_windfarm_details,load_windfarms_geojson, NoDataError, APIError
from windfarm import download_gen_data

import sys


# curvefit
from scipy.optimize import curve_fit
import json

# change the matplotlib font to use open sans
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = 'Open Sans'

COLOURS = ['#3F7D20', '#A0DDE6', '#542e71','#3F7CAC','#698F3F']

# scipy stats
from scipy import stats

def get_generation_data(bmu):
    root = os.path.dirname(os.path.abspath(__file__))
    folder_stub = f'/preprocessed_data/{bmu}'
    if not os.path.exists(root + folder_stub):
      os.makedirs(root + folder_stub)
    file_name = root + folder_stub + f'/{bmu}_generation_data.parquet'
    if os.path.exists(file_name):
      print(f"get_generation_data(): File exists: {file_name}")
      df = pd.read_parquet(file_name)
      return df
    else:
      try:
        file_list = glob.glob(root + f'/raw_gen_data/*{bmu}*.parquet')
        df = pd.DataFrame()
        for file in file_list:
            df = pd.concat([df, pd.read_parquet(file)])
        df['SP'] = df['SP'].astype('int')
        df['Quantity (MW)'] = df['Quantity (MW)'].astype('float')


        df = df.set_index('Settlement Date')
        # convert the index to a DatetimeIndex object with the London timezone
        df.index = pd.to_datetime(df.index)
        # calculate the number of minutes for each period value
        minutes = (df['SP'] - 1) * 30
        # create a new column for the local datetime values
        df['utc_time'] = df.index + pd.to_timedelta(minutes, unit='minute')
        # convert the 'local_time' column to UTC, handling daylight saving time transitions

        # make the index the 'utc_time' column
        df = df.set_index('utc_time')
        df = df.resample('30T').last()
        df.to_parquet(file_name)
        return df
      except Exception as e:
        e = f"Error getting generation data for {bmu}"
        print(e)
        return None


def get_weather_data():
    root = os.path.dirname(os.path.abspath(__file__))
    if not os.path.exists(root + '/preprocessed_data'):
      os.makedirs(root + '/preprocessed_data')
    filename = root + '/preprocessed_data/weather_data.parquet'
    if os.path.exists(filename):
      print(f"get_weather_data(): File exists: {filename}")
      df = pd.read_parquet(filename)
      return df
    else:
      file_list = glob.glob(root+ f'/weather_data/*100_*.parquet')
      df = pd.DataFrame()
      for file in file_list:
        df = pd.concat([df, pd.read_parquet(file)])
      df.to_parquet(filename)
      return df

def get_curtailment_data():
    root = os.path.dirname(os.path.abspath(__file__))
    # make a preprocesed_data folder if it doesn't exist
    if not os.path.exists(root + '/preprocessed_data'):
      os.makedirs(root + '/preprocessed_data')
    filename = root + '/preprocessed_data/curtailment_data.parquet'
    if os.path.exists(filename):
      print(f"get_curtailment_data(): File exists: {filename}")
      df = pd.read_parquet(filename)
      return df
    else:
      file_list = glob.glob(root + f'/bm_data/*BAV*.parquet')
      df = pd.DataFrame()
      for file in file_list:
        df = pd.concat([df, pd.read_parquet(file)])
      df.to_parquet(filename)
      return df
    
def get_bmu_curtailment_data(curtailment_df,bmu):
  root = os.path.dirname(os.path.abspath(__file__))
  folder_stub = f'/preprocessed_data/{bmu}'
  if not os.path.exists(root + folder_stub):
    os.makedirs(root + folder_stub)
  filename = root + folder_stub + f'/{bmu}_curtailment_data.parquet'
  if os.path.exists(filename):
     print('loading from file')
     return pd.read_parquet(filename)
  else:
  # bmu is in the BMU_id column
  # bmu stub remove the fist 2 characters of BMU_id
    filt = curtailment_df['BMU_id'].str.contains(bmu)
    # check if the bmu string is in any part of the BMU_id column
    df = curtailment_df.loc[filt].copy()
    df['Settlement Period'] = df['Settlement Period'].astype('int')
    df = df.set_index('date')
    # convert the index to a DatetimeIndex object with the London timezone
    df.index = pd.to_datetime(df.index)
    # calculate the number of minutes for each period value
    minutes = (df['Settlement Period'] - 1) * 30
    # create a new column for the local datetime values
    df['utc_time'] = df.index + pd.to_timedelta(minutes, unit='minute')
    df = df.set_index('utc_time')
    df = df.resample('30T').last()
    df['Total'] = df['Total'].astype('float')
    df.to_parquet(filename)
    return df    

def get_nearest_weather_data(weather_data,lat,lon):
    _df = weather_data.loc[(weather_data['lat'] == lat) & (weather_data['lon'] == lon)]
    # drop lat and lon columns
    _df = _df.drop(columns=['lat', 'lon'])
    # rename the index to utc_time
    _df.index.name = 'utc_time'
    _df = _df.resample('1H').last()
    return _df





def get_lat_lon(bmu,windfarm_details):
   lat, lon = windfarm_details.loc[windfarm_details['bmrs_id'] == bmu, ['lat', 'lon']].values[0]
   if lon < 0:
         lon = lon + 360
   return lat, lon

def get_capacity(bmu,windfarm_details):
    return windfarm_details.loc[windfarm_details['bmrs_id'] == bmu, 'capacity'].values[0]

def get_name(bmu,windfarm_details):
    return windfarm_details.loc[windfarm_details['bmrs_id'] == bmu, 'name'].values[0]

def get_type(bmu,windfarm_details):
    return  windfarm_details.loc[windfarm_details['bmrs_id'] == bmu, 'type'].values[0]



# a function which takes a lat, lon and returns the nearest values from a list of lat, lon values
def get_nearest_lat_lon(lat, lon):
    root = os.path.dirname(os.path.abspath(__file__))
    df = pd.read_parquet(root + '/weather_data/2023_01_northward_wind_at_100_metres.parquet')
    # get the unique lat, lon values from the dataframe and make a list
    lat_lon_list = df[['lat', 'lon']].drop_duplicates().values.tolist()
    # convert the lat, lon values to radians
    lat_rad = np.radians(lat)
    lon_rad = np.radians(lon)
    # convert the lat, lon values in the list to radians
    lat_lon_list_rad = np.radians(lat_lon_list)
    # calculate the distance between the lat, lon values and the values in the list
    # using the haversine formula
    dlon = lon_rad - lat_lon_list_rad[:, 1]
    dlat = lat_rad - lat_lon_list_rad[:, 0]
    a = np.sin(dlat / 2)**2 + np.cos(lat_rad) * np.cos(lat_lon_list_rad[:, 0]) * np.sin(dlon / 2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    # calculate the distance in km
    dist = 6371 * c
    # get the index of the minimum distance
    idx = np.argmin(dist)
    # return the lat, lon values from the list with the minimum distance
    nearest =  lat_lon_list[idx]
    nearest_lat = nearest[0]
    nearest_lon = nearest[1]
    return nearest_lat, nearest_lon


def calculate_wind_speed_and_direction(df):
    v_df = df[['northward_wind_at_100_metres']].loc[df['northward_wind_at_100_metres'].notnull()]
    # rename the variable column to 'V'
    v_df = v_df.rename(columns={'northward_wind_at_100_metres': 'V'})
    u_df = df[['eastward_wind_at_100_metres']].loc[df['eastward_wind_at_100_metres'].notnull()]
    u_df = u_df.rename(columns={'eastward_wind_at_100_metres': 'U'})
    # rename the variable column to 'V wind component'
    # merge the two dfs
    ws_df = pd.merge(v_df, u_df, on='utc_time')
    # calculate the wind speed
    ws_df['wind_speed'] = (ws_df['V']**2 + ws_df['U']**2)**0.5
    ws_df['wind_direction_degrees'] = (270 - (180/3.14159)*ws_df['V']/ws_df['U'])%360

    return ws_df[['wind_speed', 'wind_direction_degrees']]


class PCEY:
  def __init__(self, data_obj, bmu, lat, lon,capacity, name,gen_type):
    self.weather_stats_df = data_obj['weather_stats_df']
    self.ws_df = data_obj['ws_df']
    self.gen_df = data_obj['gen_df']
    self.bav_df = data_obj['bav_df']
    self.energy_yield_df = None
    self.power_curve_df = None
    self.r2 = None
    self.p50_energy_yield = None

    self.predicted_monthly_col = 'predicted_monthly_generation_GWh'
    self.ideal_yield_col = 'ideal_yield_GWh'
    self.net_yield_col = 'net_yield_GWh'
    self.daily_predicted_col = 'average_daily_predicted_yield_GWh'
    self.daily_ideal_yield_col = 'average_daily_ideal_yield_GWh'
    self.daily_net_yield_col = 'average_daily_net_yield_GWh'
    self.bmu = bmu
    self.lat = lat
    self.lon = lon
    self.capacity = capacity
    self.name = name
    self.gen_type = gen_type
    self.nearest_lat, self.nearest_lon = get_nearest_lat_lon(self.lat, self.lon)
    self._preprocess_data()

  def _preprocess_data(self):
    _bav_df = self.bav_df.copy()
    _df = self.gen_df.copy()
    _df.rename(columns={'Quantity (MW)': 'generation_GWh'}, inplace=True)
    # divide by 1000 and divide by 2
    _df[self.net_yield_col] = _df['generation_GWh'] / 2000
    month_df = _df[[self.net_yield_col]].resample('1MS').sum()
    month_df['curtailment_losses_GWh'] = -_bav_df['Total'].resample('1MS').sum()/1000.
    month_df['curtailment_losses_GWh'] = month_df['curtailment_losses_GWh'].fillna(0)
    month_df[self.ideal_yield_col] = month_df[self.net_yield_col] + month_df['curtailment_losses_GWh']

    # ge
    #sort the _df by the net yield column
    sorted_df = _df.sort_values(by=self.net_yield_col, ascending=False)
    # take the value of the 5th percentile
    max_gen = sorted_df[self.net_yield_col].iloc[int(len(sorted_df)*0.10)]

    month_df['max_net_yield_GWh'] = _df[[self.net_yield_col]].resample('1MS').max()
    # calculate the availability for each month
    month_df['availability'] = _df[self.net_yield_col].resample('1MS').count()/(month_df.index.days_in_month *48)

    month_df[self.net_yield_col+'_ok'] = np.nan 
    month_df[self.ideal_yield_col+'_ok'] = np.nan
    month_df[self.net_yield_col+'_fail'] = np.nan
    month_df[self.ideal_yield_col+'_fail'] = np.nan

    filt = (month_df['max_net_yield_GWh'] >= max_gen ) & (month_df['availability'] >= 0.70)
    month_df.loc[filt,self.net_yield_col+'_ok'] = month_df.loc[filt,self.net_yield_col]
    month_df.loc[filt,self.ideal_yield_col+'_ok'] = month_df.loc[filt,self.ideal_yield_col]

    month_df.loc[~filt,self.net_yield_col+'_fail'] = month_df.loc[~filt,self.net_yield_col]
    month_df.loc[~filt,self.ideal_yield_col+'_fail'] = month_df.loc[~filt,self.ideal_yield_col]
    # divide by the number of days in the month
    month_df[self.daily_net_yield_col] = month_df[self.net_yield_col+'_ok'] / month_df.index.days_in_month
    month_df[self.daily_ideal_yield_col] = month_df[self.ideal_yield_col+'_ok'] / month_df.index.days_in_month
    month_df[self.daily_ideal_yield_col+'_fail'] = month_df[self.ideal_yield_col+'_fail'] / month_df.index.days_in_month
    month_df[self.daily_net_yield_col+'_fail'] = month_df[self.net_yield_col+'_fail'] / month_df.index.days_in_month
    self.gen_stats_df = month_df


  def calculate_power_curve(self):
    power_curve_df = self.gen_df.copy()
    power_curve_df['wind_speed'] = ws_df[['wind_speed']]
    power_curve_df['wind_speed'] = power_curve_df['wind_speed'].interpolate()
    # power_curve_df = power_curve_df.tail(50000)
    self.power_curve_df = power_curve_df.copy()

    # describe the power curve using describe
    # convert to the nearest 0.25
    power_curve_df['rounded_wind_speed'] = power_curve_df['wind_speed'].round(0)
    power_curve_df['Quantity (MW)'] = power_curve_df['Quantity (MW)'].astype(float)

    #drop nan valuess
    power_curve_df.dropna(inplace=True)

    # initial filter

    power_curve_desc_df = power_curve_df[['Quantity (MW)', 'rounded_wind_speed']].groupby('rounded_wind_speed').describe(percentiles = [0.10, 0.90])
    power_curve_desc_df = power_curve_desc_df['Quantity (MW)'][['10%','90%', 'count']]
    filt = power_curve_desc_df['count'] > 100
    power_curve_desc_df = power_curve_desc_df.loc[filt]
    # # curve fitting
    x = power_curve_df['rounded_wind_speed'].astype(float).unique()
    # sort x 
    x.sort()
    upper_y = power_curve_desc_df['90%'].values
    lower_y = power_curve_desc_df['10%'].values
    ninety_five_percent_dict = dict(zip(x, upper_y))
    five_percent_dict = dict(zip(x, lower_y))

    
    power_curve_df['upper'] = power_curve_df['rounded_wind_speed'].map(ninety_five_percent_dict)
    power_curve_df['lower'] = power_curve_df['rounded_wind_speed'].map(five_percent_dict)
    power_curve_df.loc[power_curve_df['Quantity (MW)'] < power_curve_df['lower'], 'Quantity (MW)'] = np.nan
    power_curve_df.loc[power_curve_df['Quantity (MW)'] > power_curve_df['upper'], 'Quantity (MW)'] = np.nan
    power_curve_df.dropna(inplace=True)




    max_bound = power_curve_df['Quantity (MW)'].describe(percentiles=[0.95])['95%']*2
    # any values 1.2 times the power curve are set to nan
    
    # retrain the curve
    # drop nan values
    
    popt, pcov = curve_fit(power_curve, power_curve_df['wind_speed'], power_curve_df['Quantity (MW)'], bounds=([5, 0, -2], [max_bound, 1.6, 20]))
    self.power_curve_params = popt

    popt, pcov = curve_fit(power_curve_exp, power_curve_df['wind_speed'], power_curve_df['Quantity (MW)'], bounds=([0, -10,0], [1, 10.,max_bound]))
    self.power_curve_exp_params = popt
    
    power_curve_desc_df = power_curve_df[['Quantity (MW)', 'rounded_wind_speed']].groupby('rounded_wind_speed').describe( percentiles=[0.50])
    power_curve_desc_df = power_curve_desc_df['Quantity (MW)'][['50%', 'count']]
    filt = power_curve_desc_df['count'] > 100
    power_curve_desc_df = power_curve_desc_df.loc[filt]
    # # curve fitting
    x = power_curve_df['rounded_wind_speed'].astype(float).unique()
    # sort x 
    x.sort()
    y = power_curve_desc_df['50%'].values

    power_curve_y = power_curve(x, *self.power_curve_params ).round(1)
    power_curve_y = [power_curve_y[i] for i in range(len(y))]
    power_curve_y = [round(i, 2) for i in power_curve_y]

    power_curve_exp_y = power_curve_exp(x, *self.power_curve_exp_params ).round(1)
    power_curve_exp_y = [power_curve_exp_y[i] for i in range(len(y))]
    power_curve_exp_y = [round(i, 2) for i in power_curve_exp_y]

    # take the average of the 60% and the curve fit
    # hybrid_y = [(power_curve_y[i]*.9 + y[i]*.1) for i in range(len(y))]


    # zip up the x and y as a dict
    xy_dict = dict(zip(x, y))
    hybrid_xy_dict = dict(zip(x, power_curve_y))
    hybrid_xy_exp_dict = dict(zip(x, power_curve_exp_y))
    self.power_curve = xy_dict
    self.hybrid_curve = hybrid_xy_dict
    self.hybrid_curve_exp = hybrid_xy_exp_dict



  def get_prediction(self):
    try:
      # merge the two dfs

      self.weather_stats_df[self.daily_ideal_yield_col] = self.gen_stats_df[[self.daily_ideal_yield_col]]
      self.weather_stats_df[self.daily_net_yield_col] = self.gen_stats_df[[self.daily_net_yield_col]]
      self.weather_stats_df[self.daily_ideal_yield_col+'_fail'] = self.gen_stats_df[[self.daily_ideal_yield_col+'_fail']]
      self.weather_stats_df[self.daily_net_yield_col+'_fail'] = self.gen_stats_df[[self.daily_net_yield_col+'_fail']]
      self.weather_stats_df['curtailment_losses_GWh'] = self.gen_stats_df[['curtailment_losses_GWh']]
      # linear regression scipy
      lin_reg_df = self.weather_stats_df[['wind_speed', self.daily_ideal_yield_col]].dropna()
      net_req_df = self.weather_stats_df[['wind_speed', self.daily_net_yield_col]].dropna()
      slope, intercept, r_value, p_value, ideal_std_err = stats.linregress(lin_reg_df['wind_speed'], lin_reg_df[self.daily_ideal_yield_col])
      self.r2 = r_value**2
      self.slope = slope
      self.intercept = intercept
  

      slope, intercept, r_value, p_value, std_err = stats.linregress(net_req_df['wind_speed'], net_req_df[self.daily_net_yield_col])
      self.net_yield_r2 = r_value**2
      self.net_yield_slope = slope
      self.net_yield_intercept = intercept
      # calculate the standard error
      # convert the stndard error to a percentage





      self.weather_stats_df[self.daily_predicted_col] = self.slope * self.weather_stats_df['wind_speed'] + self.intercept
      self.weather_stats_df['top_error'] = self.weather_stats_df[self.daily_predicted_col] + ideal_std_err
      self.weather_stats_df['bottom_error'] = self.weather_stats_df[self.daily_predicted_col] - ideal_std_err

      # prediction v actual r2
      _lin_reg_df = self.weather_stats_df[[self.daily_ideal_yield_col, self.daily_predicted_col]].dropna()
      slope, intercept, r_value, p_value, std_err = stats.linregress(_lin_reg_df[self.daily_ideal_yield_col],_lin_reg_df[self.daily_predicted_col])
      self.prediction_r2 = r_value**2


      
      # add in any existing data
      # filt = self.weather_stats_df[self.daily_ideal_yield_col].notnull()
      # self.weather_stats_df.loc[filt, self.daily_predicted_col] = self.weather_stats_df.loc[filt, self.daily_ideal_yield_col]


      self.weather_stats_df[self.predicted_monthly_col] = self.weather_stats_df[self.daily_predicted_col] * self.weather_stats_df.index.days_in_month
      self.weather_stats_df['top_error'] = self.weather_stats_df['top_error'] * self.weather_stats_df.index.days_in_month
      self.weather_stats_df[self.ideal_yield_col] = self.weather_stats_df[self.daily_ideal_yield_col] * self.weather_stats_df.index.days_in_month
      self.weather_stats_df[self.net_yield_col] = self.weather_stats_df[self.daily_net_yield_col] * self.weather_stats_df.index.days_in_month
    except Exception as e:
      print('get_prediction(), error: ', e)


  def get_p50_energy_yield(self):
    try:
      # get the p50 energy yield
      _energy_yield_df = self.weather_stats_df.copy()
      _last_two_years_df = _energy_yield_df[_energy_yield_df.index.year > _energy_yield_df.index.year.max() - 2].copy()
      _last_two_years_df['month_number'] = _last_two_years_df.index.month
      _energy_yield_df['month_number'] = _energy_yield_df.index.month
      # group by month and get the median
      energy_yield_df = _energy_yield_df[[self.predicted_monthly_col,'month_number']].groupby('month_number').median()
      energy_yield_df['curtailment_losses_GWh'] = _last_two_years_df[['curtailment_losses_GWh','month_number']].groupby('month_number').mean()
      energy_yield_df['net_yield_GWh'] = energy_yield_df[self.predicted_monthly_col] - energy_yield_df['curtailment_losses_GWh']


      self.energy_yield_df = energy_yield_df
      p50_energy_yield = energy_yield_df[self.predicted_monthly_col].sum()
      self.p50_energy_yield = p50_energy_yield
      self.p50_energy_yield_net = energy_yield_df['net_yield_GWh'].sum()
      self.losses_due_to_curtailment = energy_yield_df['curtailment_losses_GWh'].sum()
      # len of month_df[self.daily_ideal_yield_col]
      self.n_data_points = len(_energy_yield_df[self.daily_ideal_yield_col].dropna())

    except Exception as e:
      print('get_p50_energy_yield(), error: ', e)

  def plot_generation(self):
    try:
      root = os.path.dirname(os.path.abspath(__file__))
      # make a folder for the plots
      plot_folder = os.path.join(root, 'plots')
      if not os.path.exists(plot_folder):
        os.mkdir(plot_folder)

      filename = f'2_{self.bmu}_pcey'
      plot_path = os.path.join(plot_folder, filename)
      plot_df = self.weather_stats_df.copy()


      fig = go.Figure()
      fig.add_trace(go.Scattergl(x=plot_df.index, y=plot_df[self.predicted_monthly_col], name='Predicted',mode='lines+markers'))
      fig.add_trace(go.Scattergl(x=plot_df.index, y=plot_df[self.ideal_yield_col], name='Generation - Curtailment', mode='lines+markers'))
      fig.add_trace(go.Scattergl(x=plot_df.index, y=plot_df[self.net_yield_col], name='Generation', mode='lines+markers'))
      title = f"<span style='font-size: 20px; font-weight: bold;'>{self.name}</span><br><span style='font-size: 16px;'>{self.bmu}</span>"
      fig.update_layout(title=title, xaxis_title='Month', yaxis_title='GWh')
      # whit theme
      fig.update_layout(template='plotly_white')
      # save to html
      fig.write_html(f'{plot_path}.html', full_html=False,config={'displayModeBar': False, 'displaylogo': False})

      plt.close()
    except Exception as e:
      print('plot_generation(), error: ', e)


  def plot_scatter(self):
    try:
      root = os.path.dirname(os.path.abspath(__file__))
      # make a folder for the plots
      plot_folder = os.path.join(root, 'plots')
      if not os.path.exists(plot_folder):
        os.mkdir(plot_folder)
      filename = f'1_{self.bmu}_scatter'
      plot_path = os.path.join(plot_folder, filename)


      # three column, 1 row subplots with Go
      from plotly.subplots import make_subplots
      colours = COLOURS
      
      #subplot 1
      fig = make_subplots(rows=3, cols=1, 
                          subplot_titles=(f"Step 1. Actual GWh v Weather Data (ERA5 Node: lat: {self.nearest_lat:.2f}, lon: {nearest_lon:.2f})", f"Step 2. (Actual GWh - Curtailed GWh) v Weather Data", f"Step 3. Predicted GWh v Actual GWh", ), 
                          vertical_spacing=0.1)           
      fig.update_annotations(font_size=12)    
      # the wind speed, production and the month in text
      ok_text = [f"QC pass<br>Wind Speed: {ws:.2f} m/s<br>Production: {prod:.2f} GWh<br>Month: {month}" for ws, prod, month in zip(self.weather_stats_df['wind_speed'], self.weather_stats_df[self.daily_net_yield_col], self.weather_stats_df.index.strftime('%b %Y'))]
      fig.add_trace(go.Scatter(x=self.weather_stats_df['wind_speed'], y=self.weather_stats_df[self.daily_net_yield_col], mode='markers', name='Auto QC pass', marker_color=colours[2],legendgroup='1',
                               hovertext=ok_text,hoverinfo='text'), row=1, col=1)
      fail_text = [f"QC fail<br>Wind Speed: {ws:.2f} m/s<br>Production: {prod:.2f} GWh<br>Month: {month}" for ws, prod, month in zip(self.weather_stats_df['wind_speed'], self.weather_stats_df[self.daily_net_yield_col + '_fail'], self.weather_stats_df.index.strftime('%b %Y'))]
      fig.add_trace(go.Scatter(x=self.weather_stats_df['wind_speed'], y=self.weather_stats_df[self.daily_net_yield_col + '_fail'], mode='markers', name='Auto QC fail', marker_color=colours[4], opacity=0.5, legendgroup='1',
                                hovertext=fail_text,hoverinfo='text'), row=1, col=1)
      fig.update_xaxes(title_text='Daily Mean Wind Speed (m/s)', row=1, col=1,titlefont=dict(size=12))
      fig.update_yaxes(title_text='Actual Daily Mean Generation (GWh)', row=1, col=1,titlefont=dict(size=12))
      # make the r^2 text

      fig.update_yaxes(range=[0,self.weather_stats_df[self.daily_ideal_yield_col].max()*1.1], row=1, col=1)
      fig.update_xaxes(range=[0,self.weather_stats_df['wind_speed'].max()*1.1], row=1, col=1)


      
      #subplot 2
      ok_text = [f"QC pass<br>Wind Speed: {ws:.2f} m/s<br>Production: {prod:.2f} GWh<br>Month: {month}" for ws, prod, month in zip(self.weather_stats_df['wind_speed'], self.weather_stats_df[self.daily_ideal_yield_col], self.weather_stats_df.index.strftime('%b %Y'))]
      fig.add_trace(go.Scatter(x=self.weather_stats_df['wind_speed'], y=self.weather_stats_df[self.daily_ideal_yield_col], mode='markers', name='Auto QC pass', marker_color=colours[2],legendgroup='2'
                                ,hovertext=ok_text,hoverinfo='text', showlegend=False), row=2, col=1)
      fail_text = [f"QC fail<br>Wind Speed: {ws:.2f} m/s<br>Production: {prod:.2f} GWh<br>Month: {month}" for ws, prod, month in zip(self.weather_stats_df['wind_speed'], self.weather_stats_df[self.daily_ideal_yield_col + '_fail'], self.weather_stats_df.index.strftime('%b %Y'))]
      fig.add_trace(go.Scatter(x=self.weather_stats_df['wind_speed'], y=self.weather_stats_df[self.daily_ideal_yield_col + '_fail'], mode='markers', name='Auto QC fail', marker_color=colours[4], opacity=0.5, legendgroup='2',
                                hovertext=fail_text,hoverinfo='text', showlegend=False), row=2, col=1)

      if self.intercept > 0:
        text = f'y = {self.slope:.2f}x + {abs(self.intercept):.2f}'
      else:
        text = f'y = {self.slope:.2f}x - {abs(self.intercept):.2f}'
      x_range = np.arange(0, self.weather_stats_df['wind_speed'].max()*1.1, 0.1)
      fig.add_trace(go.Scatter(x=x_range, y=self.slope * x_range + self.intercept, mode='lines', name=text, line=dict(color=colours[3], dash='dash')), row=2, col=1)
      fig.update_xaxes(title_text='Daily Mean Wind Speed (m/s)', row=2, col=1,titlefont=dict(size=12))
      fig.update_yaxes(title_text='Actual Daily Mean Generation (GWh) - Curtailed GWh', row=2, col=1,titlefont=dict(size=12))
      fig.update_yaxes(range=[0,self.weather_stats_df[self.daily_ideal_yield_col].max()*1.1], row=2, col=1)
      fig.update_xaxes(range=[0,self.weather_stats_df['wind_speed'].max()*1.1], row=2, col=1)

      #subplot 3
      text = [f"Predicted: {pred:.2f} GWh<br>Actual: {actual:.2f} GWh<br>Month: {month}" for pred, actual, month in zip(self.weather_stats_df[self.daily_predicted_col], self.weather_stats_df[self.daily_ideal_yield_col], self.weather_stats_df.index.strftime('%b %Y'))]
      fig.add_trace(go.Scatter(x=self.weather_stats_df[self.daily_predicted_col], y=self.weather_stats_df[self.daily_ideal_yield_col], mode='markers',marker_color = colours[2],legendgroup='3',
                                hovertext=text,hoverinfo='text', showlegend=False), row=3, col=1)
      _lin_reg_df = self.weather_stats_df[[self.daily_predicted_col, self.daily_ideal_yield_col]].dropna()
      # linear regression between predicted and actual
      slope, intercept, r_value, p_value, ideal_std_err = stats.linregress(_lin_reg_df[self.daily_predicted_col], _lin_reg_df[self.daily_ideal_yield_col])
      x_range = np.arange(0, self.weather_stats_df[self.daily_predicted_col].max()*1.1, 0.1)
      r2_text = f'R^2 = {r_value**2:.2f}'

      fig.add_trace(go.Scatter(x=x_range, y=slope * x_range + intercept, mode='lines', name=r2_text, line=dict(color='red', dash='dash')), row=3, col=1)
      fig.update_xaxes(title_text='Predicted Daily Mean Generation (GWh)', row=3, col=1 ,titlefont=dict(size=12))
      fig.update_yaxes(title_text='Actual Daily Mean Generation (GWh)', row=3, col=1,titlefont=dict(size=12))
      fig.update_yaxes(range=[0,self.weather_stats_df[self.daily_ideal_yield_col].max()*1.1], row=3, col=1)
      fig.update_xaxes(range=[0,self.weather_stats_df[self.daily_predicted_col].max()*1.1], row=3, col=1)

      
      # set the legend to be 'h'
      fig.update_layout(legend_orientation="h")
      # center the legend
      fig.update_layout(legend=dict(x=0.5, y=-0.1, xanchor='center', yanchor='top'))
      # set the height to be 800
      fig.update_layout(height=1500)
      # set the max width to be 1000
      # remove padding from the plot
      fig.update_layout(margin=dict(l=0, r=0))

      # plotl white themw
      fig.update_layout(template='plotly_white')
      title = f"<span style='font-size: 16; font-weight: bold;'>{self.name}, {self.bmu}<br>Least squares model</span>"
      # title = f'{self.name}, {self.bmu} scatter. Prediction v Actual'
      fig.update_layout(title_text=title, title_x=0.5, title_font_size=16)
      fig.update_layout(
        dragmode=False)
            # reduce space between subplots
      # turn off 


      # save the plot
      fig.write_html(f"{plot_path}.html", full_html=False,config={'displayModeBar': False, 'displaylogo': False})


    


    except Exception as e:
      print('plot_scatter(), error: ', e)




  def plot_p50(self):
    try:
      root = os.path.dirname(os.path.abspath(__file__))
      # make a folder for the plots
      plot_folder = os.path.join(root, 'plots')
      if not os.path.exists(plot_folder):
        os.mkdir(plot_folder)
      filename = f'3_{self.bmu}_p50'
      plot_path = os.path.join(plot_folder, filename)
      fig = plt.figure(figsize=(10,5))
      ax = fig.add_subplot(111)
      
      ax.bar(self.energy_yield_df.index-0.15,width=0.3, height = self.energy_yield_df[self.predicted_monthly_col], label='Predicted (without curtailment losses)')

      ax.bar(self.energy_yield_df.index+0.15,width=0.3,height= self.energy_yield_df['net_yield_GWh'], label='Predicted (with curtailment losses)')
      # make the ticks 'Jan', 'Feb' as opposed to 1, 2
      labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
      ax.legend()
      ax.set_xticks(self.energy_yield_df.index)
      ax.set_xticklabels(labels)

      ax.set_xlabel('Month')
      ax.set_ylabel('P50 Energy Yield (GWh)')
      # r2 text

      fig.suptitle(f'{self.bmu}, Capacity {self.capacity:.0f}MW Annual P50 Energy Yield: {self.p50_energy_yield:.0f} GWh, number of months: {self.n_data_points}\nModel $r^2$: {self.prediction_r2:.2f}')
      self.losses_as_percentage = f'{self.losses_due_to_curtailment / self.p50_energy_yield * 100:.2f}%'
      losses_text = f'Annual observed losses due to curtailment: {self.losses_due_to_curtailment:.0f} GWh {self.losses_as_percentage}'
      ax.set_title(losses_text)

      fig.savefig(f"{plot_path}.png")
      plt.close()
      fig = go.Figure()
      fig.add_trace(go.Bar(x=self.energy_yield_df.index-0.15,width=0.3, y = self.energy_yield_df[self.predicted_monthly_col], name='Predicted (without curtailment losses)'))
      fig.add_trace(go.Bar(x=self.energy_yield_df.index+0.15,width=0.3,y= self.energy_yield_df['net_yield_GWh'], name='Predicted (with curtailment losses)'))
      # make the ticks 'Jan', 'Feb' as opposed to 1, 2
      labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
      # add the labels to the x axis
      fig.update_xaxes(ticktext=labels, tickvals=self.energy_yield_df.index)
      fig.update_layout(legend_orientation="h")
      # add y axis labels
      fig.update_yaxes(title_text='Monthly Energy Yield (GWh)')
      # center the legend
      fig.update_layout(legend=dict(x=0.5, y=-0.1, xanchor='center', yanchor='top'))
      # set the height to be 800
      fig.update_layout(height=600)
      # set the max width to be 1000
      # remove padding from the plot
      fig.update_layout(margin=dict(l=0, r=0))

      # plotl white themw
      fig.update_layout(template='plotly_white')
      # plotly latex r2
      title=f"""<b style = 'font-size:16px'>BMU: {self.bmu}<br>Annual Energy Yield: {self.p50_energy_yield:.0f} GWh</b>
      <br><span style='font-size:14px'>number of months: {self.n_data_points}, model r-squared: {self.prediction_r2:.2f}</span>"""
      fig.update_layout(title_text=title, title_x=0.5, title_font_size=16)
      fig.update_layout(
        dragmode=False)
            # reduce space between subplots

      fig.write_html(f"{plot_path}.html", full_html=False,config={'displayModeBar': False, 'displaylogo': False})
    
    except Exception as e:
      print('plot_p50(), error: ', e)


  def plot_power_curve(self):
    try:
      root = os.path.dirname(os.path.abspath(__file__))
      # make a folder for the plots
      plot_folder = os.path.join(root, 'plots')
      if not os.path.exists(plot_folder):
        os.mkdir(plot_folder)
      filename = f'{self.bmu}_power_curve.png'
      plot_path = os.path.join(plot_folder, filename)
      fig = plt.figure(figsize=(8,8))
      ax = fig.add_subplot(111)
      if self.power_curve_df is None:
        self.calculate_power_curve()
      

      colours = matplotlib.cm.get_cmap('viridis')(np.linspace(0, 1, 4))
      
      ax.scatter(self.power_curve_df['wind_speed'],self.power_curve_df['Quantity (MW)'],s=0.1, alpha=1, c = colours[0])
      # plot the power curve from 0 to the max wind speed use the dictionary
      x_values = [x for x in self.power_curve.keys()]
      y_values = [self.power_curve[x] for x in self.power_curve.keys()]
      # ax.plot(x_values, y_values, color='red', label = 'data derived')

      curve_fit = power_curve(x_values, *self.power_curve_params)
      
      exp_fit = [power_curve_exp(x, *self.power_curve_exp_params) for x in x_values]
      # math text for a logistic function
      logistic_text = r'$\frac{a}{1+e^{-b(x-c)}}$'
      exp_text = r'$e^{ax+b}$'
      # hybrid = [curve_fit[i] if curve_fit[i] > hybrid[i] else hybrid[i] for i in range(len(hybrid))]
      ax.plot(x_values, curve_fit, label = f'curve fit: {logistic_text}', linewidth=3, color=colours[1])
      ax.plot(x_values, exp_fit, label = f'exp fit: {exp_text}', linewidth=3, color=colours[2])
      ax.plot(x_values, (exp_fit+curve_fit)/2, label = 'hybrid', linewidth=3, color=colours[3])
      ax.legend()


      ax.set_xlabel('Wind Speed (m/s)')
      ax.set_ylabel('Power (MW)')
      ax.set_title(f'{self.name}, {self.bmu} Power Curve')
      ax.set_xlim(0)
      ax.set_ylim(0)
      ax.grid(alpha=0.5)
      fig.savefig(plot_path)
      plt.close()
    except Exception as e:
      print('plot_power_curve(), error: ', e)


# # sigmoid function for power curve
def power_curve(x, a, b, c):
  return a / (1 + np.exp(-b * (x - c)))

def linear_curve(x, a, b):
  return a * x + b

# exponential function for power curve
def power_curve_exp(x, a, b,c):
  out = np.exp(a * x + b)
  return np.clip(out, 0, c)
  # smooth the curve as a function of distance from x = 0

def get_weather_stats_df(weather_df):
  weather_stats_df = weather_df[['wind_speed']].resample('1D').mean()
  weather_stats_df = weather_stats_df[['wind_speed']].resample('1MS').mean()
  return weather_stats_df
     
if __name__ == "__main__":
  root = os.path.dirname(os.path.abspath(__file__))

  windfarm_details = load_windfarms_geojson()
  filt = windfarm_details['capacity'] > 1
  windfarm_details = windfarm_details[filt]
  weather_df = get_weather_data()
  curtailment_df = get_curtailment_data()
  common_data_obj = {}
  common_data_obj['weather_df'] = weather_df
  common_data_obj['curtailment_df'] = curtailment_df
  # drop where bmrs_id is null
  windfarm_details.dropna(subset=['bmrs_id'], inplace=True)
  rows  = []
  # power_curve_dict = {}
  for index, row in windfarm_details.iterrows():
    
    lat, lon = row['lat'], row['lon']
    cap = row['capacity']
    bmus = row['bmrs_id']
    name = row['name']
    gen_type = 'Wind Farm'

      # SGRWO-2
    for bmu in bmus:
      
      try:
        # assert bmu == 'CRYRW-3'
        # get the nearest lat and lon
        nearest_lat, nearest_lon=  get_nearest_lat_lon(lat, lon)
        weather_df = get_nearest_weather_data(common_data_obj['weather_df'], nearest_lat, nearest_lon)
        

        bav_df = get_bmu_curtailment_data(common_data_obj['curtailment_df'], bmu)
        ws_df = calculate_wind_speed_and_direction(weather_df)

        weather_stats_df = get_weather_stats_df(ws_df)

        gen_df = get_generation_data(bmu)
        try:
          assert len(gen_df) > 0
        except:
          raise NoDataError(f'No generation data for {bmu}')
        
        data_obj ={}
        data_obj['weather_stats_df'] = weather_stats_df
        data_obj['ws_df'] = ws_df
        data_obj['gen_df'] = gen_df
  
        data_obj['bav_df'] = bav_df
        wf_pcey = PCEY(data_obj, bmu, lat, lon, cap, name, gen_type)
        wf_pcey.get_prediction()
        wf_pcey.get_p50_energy_yield()
        wf_pcey.plot_generation()
        wf_pcey.plot_scatter()
        # wf_pcey.plot_scatter_v2()
        wf_pcey.plot_p50()
        pcey_dict = {}
        pcey_dict['bmu'] = bmu
        pcey_dict['p50_energy_yield'] = wf_pcey.p50_energy_yield
        pcey_dict['curtailment'] = wf_pcey.losses_due_to_curtailment
        pcey_dict['GWh/MW'] = wf_pcey.p50_energy_yield/ wf_pcey.capacity
        pcey_dict['curtailment %'] = wf_pcey.losses_as_percentage
        pcey_dict['n_data_points'] = wf_pcey.n_data_points
        pcey_dict['r2'] = wf_pcey.prediction_r2
        pcey_dict['lat'] = wf_pcey.lat
        pcey_dict['lon'] = wf_pcey.lon
        pcey_dict['capacity'] = wf_pcey.capacity
        pcey_dict['name'] = wf_pcey.name
        pcey_dict['gen_type'] = wf_pcey.gen_type

        rows.append(pcey_dict)
    

      except Exception as e:
        # tell me which line it failed using sys.exc_info
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error on line {exc_tb.tb_lineno}: {e}")
    df = pd.DataFrame(rows)
    df.to_csv('pcey.csv', index=False)

    # # save the power curve dictionary
    # power_curve_dict_path = os.path.join(root, 'power_curve_dict.json')
    # with open(power_curve_dict_path, 'w') as f:
    #   json.dump(power_curve_dict, f)

