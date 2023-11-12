import ast
import datetime as dt
import glob
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import plotly.graph_objects as go
import sys


# Custom modules
from src.energy_yield.pcey import PCEY
import src.utils.helpers as helpers
import src.data_handling.weather as weather
import src.data_handling.bmrs as bmrs

# curvefit


# scipy stats
from scipy import stats

def tell_me_where_i_am():
   # where am I?
    print(os.path.dirname(os.path.abspath(__file__)))
    # print 1 level up
    print(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return None








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
     
def enforce_list(_string_list):
    try:
      assert isinstance(_string_list, list)
    except:
      string_list = ast.literal_eval(_string_list)
    return string_list

if __name__ == "__main__":
   
  


  windfarm_details = helpers.read_custom_windfarm_csv()
  filt = windfarm_details['capacity'] > 1
  windfarm_details = windfarm_details[filt]
  weather_df = weather.fetch_all_weather_data()
  curtailment_df = bmrs.fetch_all_curtailment_data()
  common_data_obj = {}
  common_data_obj['weather_df'] = weather_df
  common_data_obj['curtailment_df'] = curtailment_df
  # drop where bmrs_id is null
  windfarm_details.dropna(subset=['bmrs_id'], inplace=True)
  # get the duplicated rows of bmrs_id and name
  filt = windfarm_details.duplicated(subset=['bmrs_id', 'name'], keep='first')
  # drop the duplicated rows
  windfarm_details = windfarm_details[~filt]
  rows  = []
  # power_curve_dict = {}
  for index, row in windfarm_details.iterrows():
    
    lat, lon = row['lat'], row['lon']
    cap = row['capacity']
    bmus = enforce_list(row['bmrs_id']) 
    name = row['name']
    gen_type = row['gen_type']

      # SGRWO-2
    for bmu in bmus:
      
      try:
        # assert bmu == 'CRYRW-3'
        # get the nearest lat and lon
        nearest_lat, nearest_lon=  helpers.get_nearest_lat_lon(lat, lon)
        weather_df = helpers.get_nearest_weather_data(common_data_obj['weather_df'], nearest_lat, nearest_lon)
        

        bav_df = bmrs.get_bmu_curtailment_data(common_data_obj['curtailment_df'], bmu)
        ws_df = calculate_wind_speed_and_direction(weather_df)

        weather_stats_df = get_weather_stats_df(ws_df)

        gen_df = bmrs.get_generation_data(bmu)
        try:
          assert len(gen_df) > 0
        except:
          raise helpers.NoDataError(f'No generation data for {bmu}')
        
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


