import pandas as pd
import numpy as np
from scipy import stats

# random forest regressor
from sklearn.ensemble import RandomForestRegressor
# test train split
from sklearn.model_selection import train_test_split


import os
import plotly.graph_objects as go
import matplotlib.pyplot as plt
from plotly.subplots import make_subplots
from utils.helpers import get_nearest_lat_lon


### plotting stuff



# change the matplotlib font to use open sans
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = 'Open Sans'

COLOURS = ['#3F7D20', '#A0DDE6', '#542e71','#3F7CAC','#698F3F']

global project_root_path
project_root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class PCEY:
	def __init__(self, data_obj, bmu, lat, lon, capacity, name, gen_type):
		self.weather_stats_df = data_obj['weather_stats_df']
		self.ws_df = data_obj['ws_df']
		self.gen_df = data_obj['gen_df']
		self.bav_df = data_obj['bav_df']
		self.preprocessed_df = None
		self.energy_yield_df = None
		self.power_curve_df = None
		self.month_df = None
		self.r2 = None
		self.p50_energy_yield = None
		self.prediction_ok = False

		self.COL_PREDICTED_MONTHLY = 'predicted_monthly_generation_GWh'
		self.COL_IDEAL_YIELD = 'ideal_yield_GWh'
		self.COL_PREDICTED_IDEAL_YIELD = 'predicted_ideal_yield_GWh'
		self.COL_NET_YIELD = 'net_yield_GWh'
		self.COL_CURTAILMENT_LOSSES = 'curtailment_losses_GWh'
		self.COL_QCd_YIELD = 'combined_yield_GWh'

		self.COL_DAILY_PREDICTED = 'average_daily_predicted_yield_GWh'
		self.COL_DAILY_IDEAL_YIELD = 'average_daily_ideal_yield_GWh'
		self.COL_DAILY_NET_YIELD = 'average_daily_net_yield_GWh'
		self.COL_DAILY_QCd_YIELD = 'average_daily_combined_yield_GWh'
	
		self.bmu = bmu
		self.lat = lat
		self.lon = lon
		self.capacity = capacity
		self.name = name
		self.gen_type = gen_type
		self.nearest_lat, self.nearest_lon = get_nearest_lat_lon(self.lat, self.lon)

	def get_ml_prediction(self):
		try:
			if self.preprocessed_df is None:
				self._preprocess_data()
			# test train split
			ml_df = self.preprocessed_df[['wind_speed', 'wind_direction_degrees', self.COL_IDEAL_YIELD]].dropna().copy()
			# any values where the ideal yield is 0, drop them
			ml_df = ml_df[ml_df[self.COL_IDEAL_YIELD] != 0]
			# resample to the median for the day
			X = ml_df[['wind_speed', 'wind_direction_degrees']]

			y = ml_df[self.COL_IDEAL_YIELD]
			X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
			# fit the model
			model = RandomForestRegressor()
			model.fit(X_train, y_train)
			# get the predictions
			y_pred = model.predict(X_test)
			# get the r2 using scipy
			slope, intercept, r_value, p_value, std_err = stats.linregress(y_test, y_pred)
			print('r2: ', r_value**2)
			self.preprocessed_df[self.COL_PREDICTED_IDEAL_YIELD] = np.nan
			# where the feature cols are not null, predict the ideal yield
			filt = self.preprocessed_df[['wind_speed', 'wind_direction_degrees']].notnull().all(axis=1)
			self.preprocessed_df.loc[filt, self.COL_PREDICTED_IDEAL_YIELD] = model.predict(self.preprocessed_df.loc[filt, ['wind_speed', 'wind_direction_degrees']])# 
			self.prediction_ok = True
		except Exception as e:
			print('get_ml_prediction(), error: ', e)
			self.prediction_ok = False
		

	


	def _preprocess_data(self):
		# merge bav, gen and weather data
		
		merged_df= self.ws_df[['wind_speed']].resample('30T').last().copy()
		merged_df['wind_direction_degrees'] = self.ws_df[['wind_direction_degrees']].resample('30T').last().copy()
		merged_df['Quantity (MW)'] = self.gen_df[['Quantity (MW)']].resample('30T').last().copy()
		merged_df['Total'] = self.bav_df[['Total']].resample('30T').last().fillna(0).copy()
		# interpolate the ws_df
		for col in ['wind_speed', 'wind_direction_degrees']:
			merged_df[col].interpolate(method='linear', inplace=True, limit=2)
		merged_df[self.COL_NET_YIELD] = merged_df['Quantity (MW)'] / 2000.
		merged_df[self.COL_CURTAILMENT_LOSSES] = -merged_df['Total'] / 1000.
		# drop the total column and the quantity column
		merged_df.drop(columns=['Total', 'Quantity (MW)'], inplace=True)
		# fill the na values with 0
		# for col in [self.COL_NET_YIELD, self.COL_CURTAILMENT_LOSSES]:
		# 	merged_df[col].fillna(0., inplace=True, limit_direction='forward')
		merged_df[self.COL_IDEAL_YIELD] = merged_df[self.COL_NET_YIELD].fillna(0) + merged_df[self.COL_CURTAILMENT_LOSSES].fillna(0) 
		# remove the self.col_ideal_yield where 0
		# merged_df.loc[merged_df[self.COL_IDEAL_YIELD] == 0, self.COL_IDEAL_YIELD] = np.nan
		self.preprocessed_df = merged_df.copy()


	

	def auto_qc(self):
		if self.preprocessed_df is None:
			self._preprocess_data()
		if self.prediction_ok:
			_df = self.preprocessed_df.copy()

			# group by month
			month_df = _df[[self.COL_NET_YIELD, self.COL_IDEAL_YIELD, self.COL_CURTAILMENT_LOSSES, self.COL_PREDICTED_IDEAL_YIELD]].resample('1MS').sum()

			# calculate the availability for each month
			month_df['data_coverage_%'] = _df[self.COL_NET_YIELD].resample('1MS').count()/(month_df.index.days_in_month * 48)*100.
			month_df[self.COL_NET_YIELD+'_ok'] = np.nan 
			month_df[self.COL_IDEAL_YIELD+'_ok'] = np.nan
			month_df[self.COL_NET_YIELD+'_fail'] = np.nan
			month_df[self.COL_IDEAL_YIELD+'_fail'] = np.nan

			filt = (month_df['data_coverage_%'] >= 70)
			# filt = (month_df['availability'] >= 0.6)
			month_df.loc[filt, self.COL_NET_YIELD+'_ok'] = month_df.loc[filt, self.COL_NET_YIELD]
			month_df.loc[filt, self.COL_IDEAL_YIELD+'_ok'] = month_df.loc[filt, self.COL_IDEAL_YIELD]

			month_df.loc[~filt, self.COL_NET_YIELD+'_fail'] = month_df.loc[~filt, self.COL_NET_YIELD]
			month_df.loc[~filt, self.COL_IDEAL_YIELD+'_fail'] = month_df.loc[~filt, self.COL_IDEAL_YIELD]


			month_df[self.COL_QCd_YIELD] = month_df[self.COL_IDEAL_YIELD+'_ok']
			month_df.loc[month_df[self.COL_IDEAL_YIELD+'_ok'].isnull(), self.COL_QCd_YIELD] = month_df.loc[month_df[self.COL_IDEAL_YIELD+'_ok'].isnull(), self.COL_PREDICTED_IDEAL_YIELD]
			# divide by the number of days in the month
			month_df[self.COL_DAILY_NET_YIELD] = month_df[self.COL_NET_YIELD+'_ok'] / month_df.index.days_in_month
			month_df[self.COL_DAILY_IDEAL_YIELD] = month_df[self.COL_IDEAL_YIELD+'_ok'] / month_df.index.days_in_month
			month_df[self.COL_DAILY_PREDICTED] = month_df[self.COL_PREDICTED_IDEAL_YIELD] / month_df.index.days_in_month
			month_df[self.COL_DAILY_QCd_YIELD] = month_df[self.COL_QCd_YIELD] / month_df.index.days_in_month
			
			# make a combined column of the predicted and actual ideal yield_ok


			month_df[self.COL_DAILY_IDEAL_YIELD+'_fail'] = month_df[self.COL_IDEAL_YIELD+'_fail'] / month_df.index.days_in_month
			month_df[self.COL_DAILY_NET_YIELD+'_fail'] = month_df[self.COL_NET_YIELD+'_fail'] / month_df.index.days_in_month
			month_df[self.COL_DAILY_IDEAL_YIELD+'_ok'] = month_df[self.COL_IDEAL_YIELD+'_ok'] / month_df.index.days_in_month
			month_df[self.COL_DAILY_NET_YIELD+'_ok'] = month_df[self.COL_NET_YIELD+'_ok'] / month_df.index.days_in_month
			self.month_df = month_df.copy()


	def calculate_energy_yield(self):
		if self.month_df is None:
			self.auto_qc()
		try:
		# merge the two dfs

			self.weather_stats_df[self.COL_DAILY_IDEAL_YIELD] = self.month_df[[self.COL_DAILY_IDEAL_YIELD]]
			self.weather_stats_df[self.COL_DAILY_NET_YIELD] = self.month_df[[self.COL_DAILY_NET_YIELD]]
			self.weather_stats_df[self.COL_DAILY_IDEAL_YIELD+'_fail'] = self.month_df[[self.COL_DAILY_IDEAL_YIELD+'_fail']]
			self.weather_stats_df[self.COL_DAILY_NET_YIELD+'_fail'] = self.month_df[[self.COL_DAILY_NET_YIELD+'_fail']]
			self.weather_stats_df[self.COL_DAILY_IDEAL_YIELD+'_ok'] = self.month_df[[self.COL_DAILY_IDEAL_YIELD+'_ok']]
			self.weather_stats_df[self.COL_DAILY_NET_YIELD+'_ok'] = self.month_df[[self.COL_DAILY_NET_YIELD+'_ok']]
			self.weather_stats_df[self.COL_DAILY_QCd_YIELD] = self.month_df[[self.COL_DAILY_QCd_YIELD]]


			self.weather_stats_df[self.COL_DAILY_PREDICTED] = self.month_df[[self.COL_DAILY_PREDICTED]]
			self.weather_stats_df['curtailment_losses_GWh'] = self.month_df[['curtailment_losses_GWh']]
			# linear regression scipy
			lin_reg_df = self.weather_stats_df[['wind_speed', self.COL_DAILY_IDEAL_YIELD]].dropna()
			net_req_df = self.weather_stats_df[['wind_speed', self.COL_DAILY_NET_YIELD]].dropna()
			slope, intercept, r_value, p_value, ideal_std_err = stats.linregress(lin_reg_df['wind_speed'], lin_reg_df[self.COL_DAILY_IDEAL_YIELD])
			self.r2 = r_value**2
			self.slope = slope
			self.intercept = intercept
		

			slope, intercept, r_value, p_value, std_err = stats.linregress(net_req_df['wind_speed'], net_req_df[self.COL_DAILY_NET_YIELD])
			self.net_yield_r2 = r_value**2
			self.net_yield_slope = slope
			self.net_yield_intercept = intercept
			# calculate the standard error
			# convert the stndard error to a percentage

			self.weather_stats_df['top_error'] = self.weather_stats_df[self.COL_DAILY_PREDICTED] + ideal_std_err
			self.weather_stats_df['bottom_error'] = self.weather_stats_df[self.COL_DAILY_PREDICTED] - ideal_std_err

			# prediction v actual r2
			_lin_reg_df = self.weather_stats_df[[self.COL_DAILY_IDEAL_YIELD, self.COL_DAILY_PREDICTED]].dropna()
			slope, intercept, r_value, p_value, std_err = stats.linregress(_lin_reg_df[self.COL_DAILY_IDEAL_YIELD],_lin_reg_df[self.COL_DAILY_PREDICTED])
			self.prediction_r2 = r_value**2


			
			# add in any existing data
			# filt = self.weather_stats_df[self.daily_ideal_yield_col].notnull()
			# self.weather_stats_df.loc[filt, self.daily_predicted_col] = self.weather_stats_df.loc[filt, self.daily_ideal_yield_col]


			self.weather_stats_df[self.COL_PREDICTED_MONTHLY] = self.weather_stats_df[self.COL_DAILY_PREDICTED] * self.weather_stats_df.index.days_in_month
			self.weather_stats_df['top_error'] = self.weather_stats_df['top_error'] * self.weather_stats_df.index.days_in_month
			self.weather_stats_df[self.COL_IDEAL_YIELD] = self.weather_stats_df[self.COL_DAILY_IDEAL_YIELD] * self.weather_stats_df.index.days_in_month
			self.weather_stats_df[self.COL_NET_YIELD] = self.weather_stats_df[self.COL_DAILY_NET_YIELD] * self.weather_stats_df.index.days_in_month
			self.weather_stats_df[self.COL_QCd_YIELD] = self.weather_stats_df[self.COL_DAILY_QCd_YIELD] * self.weather_stats_df.index.days_in_month
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
			energy_yield_df = _energy_yield_df[[self.COL_QCd_YIELD, 'month_number']].groupby('month_number').median()
			energy_yield_df['curtailment_losses_GWh'] = _last_two_years_df[['curtailment_losses_GWh', 'month_number']].groupby('month_number').mean()
			energy_yield_df['net_yield_GWh'] = energy_yield_df[self.COL_QCd_YIELD] - energy_yield_df['curtailment_losses_GWh']
			
			self.energy_yield_df = energy_yield_df
			p50_energy_yield = energy_yield_df[self.COL_QCd_YIELD].sum()
			
			self.p50_energy_yield = p50_energy_yield
			self.p50_energy_yield_net = energy_yield_df['net_yield_GWh'].sum()
			self.losses_due_to_curtailment = energy_yield_df['curtailment_losses_GWh'].sum()
			self.losses_as_percentage = f'{self.losses_due_to_curtailment / self.p50_energy_yield * 100:.2f}%'
			
			# len of month_df[self.daily_ideal_yield_col]
			self.n_data_points = len(_energy_yield_df[self.COL_DAILY_IDEAL_YIELD].dropna())
		except Exception as e:
			print('get_p50_energy_yield(), error: ', e)


	def plot_generation(self):
		try:
			# make a folder for the plots
			plot_folder = os.path.join(project_root_path, 'plots')
			if not os.path.exists(plot_folder):
				os.mkdir(plot_folder)

			filename = f'2_{self.bmu}_pcey'
			plot_path = os.path.join(plot_folder, filename)
			plot_df = self.weather_stats_df.copy()


			fig = go.Figure()
			fig.add_trace(go.Scattergl(x=plot_df.index, y=plot_df[self.COL_PREDICTED_MONTHLY], name='Predicted',mode='lines+markers'))
			fig.add_trace(go.Scattergl(x=plot_df.index, y=plot_df[self.COL_NET_YIELD], name='Generation', mode='lines+markers'))
			fig.add_trace(go.Scattergl(x=plot_df.index, y=plot_df[self.COL_IDEAL_YIELD], name='Generation - Curtailment', mode='lines+markers'))
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
			plot_df = self.weather_stats_df.copy()
			filt = plot_df[self.COL_DAILY_IDEAL_YIELD + '_ok'] > 0
			filt2 = plot_df[self.COL_DAILY_IDEAL_YIELD + '_fail'] > 0
			plot_df = plot_df[filt | filt2].copy()

			# make a folder for the plots
			plot_folder = os.path.join(project_root_path, 'plots')
			if not os.path.exists(plot_folder):
				os.mkdir(plot_folder)
			filename = f'1_{self.bmu}_scatter'
			plot_path = os.path.join(plot_folder, filename)


			# three column, 1 row subplots with Go

			colours = COLOURS
			
			#subplot 1
			fig = make_subplots(rows=3, cols=1, 
								subplot_titles=(f"Step 1. Actual GWh v Weather Data (ERA5 Node: lat: {self.nearest_lat:.2f}, lon: {self.nearest_lon:.2f})", f"Step 2. (Actual GWh - Curtailed GWh) v Weather Data", f"Step 3. Predicted GWh v Actual GWh", ), 
								vertical_spacing=0.1)           
			fig.update_annotations(font_size=12)    
			# the wind speed, production and the month in text
			ok_text = [f"QC pass<br>Wind Speed: {ws:.2f} m/s<br>Production: {prod:.2f} GWh<br>Month: {month}" for ws, prod, month in zip(plot_df['wind_speed'], plot_df[self.COL_DAILY_NET_YIELD], plot_df.index.strftime('%b %Y'))]
			fig.add_trace(go.Scatter(x=plot_df['wind_speed'], y=plot_df[self.COL_DAILY_NET_YIELD], mode='markers', name='Auto QC pass', marker_color=colours[2],legendgroup='1',
									hovertext=ok_text,hoverinfo='text'), row=1, col=1)
			fail_text = [f"QC fail<br>Wind Speed: {ws:.2f} m/s<br>Production: {prod:.2f} GWh<br>Month: {month}" for ws, prod, month in zip(plot_df['wind_speed'], plot_df[self.COL_DAILY_NET_YIELD + '_fail'], plot_df.index.strftime('%b %Y'))]
			fig.add_trace(go.Scatter(x=plot_df['wind_speed'], y=plot_df[self.COL_DAILY_NET_YIELD + '_fail'], mode='markers', name='Auto QC fail', marker_color=colours[4], opacity=0.5, legendgroup='1',
										hovertext=fail_text,hoverinfo='text'), row=1, col=1)
			fig.update_xaxes(title_text='Daily Mean Wind Speed (m/s)', row=1, col=1,titlefont=dict(size=12))
			fig.update_yaxes(title_text='Actual Daily Mean Generation (GWh)', row=1, col=1,titlefont=dict(size=12))
			# make the r^2 text

			fig.update_yaxes(range=[0,plot_df[self.COL_DAILY_IDEAL_YIELD].max()*1.1], row=1, col=1)
			fig.update_xaxes(range=[0,plot_df['wind_speed'].max()*1.1], row=1, col=1)


			
			#subplot 2
			ok_text = [f"QC pass<br>Wind Speed: {ws:.2f} m/s<br>Production: {prod:.2f} GWh<br>Month: {month}" for ws, prod, month in zip(plot_df['wind_speed'], plot_df[self.COL_DAILY_IDEAL_YIELD], plot_df.index.strftime('%b %Y'))]
			fig.add_trace(go.Scatter(x=plot_df['wind_speed'], y=plot_df[self.COL_DAILY_IDEAL_YIELD], mode='markers', name='Auto QC pass', marker_color=colours[2],legendgroup='2'
										,hovertext=ok_text,hoverinfo='text', showlegend=False), row=2, col=1)
			fail_text = [f"QC fail<br>Wind Speed: {ws:.2f} m/s<br>Production: {prod:.2f} GWh<br>Month: {month}" for ws, prod, month in zip(plot_df['wind_speed'], plot_df[self.COL_DAILY_IDEAL_YIELD + '_fail'], plot_df.index.strftime('%b %Y'))]
			fig.add_trace(go.Scatter(x=plot_df['wind_speed'], y=plot_df[self.COL_DAILY_IDEAL_YIELD + '_fail'], mode='markers', name='Auto QC fail', marker_color=colours[4], opacity=0.5, legendgroup='2',
										hovertext=fail_text,hoverinfo='text', showlegend=False), row=2, col=1)

			if self.intercept > 0:
				text = f'y = {self.slope:.2f}x + {abs(self.intercept):.2f}'
			else:
				text = f'y = {self.slope:.2f}x - {abs(self.intercept):.2f}'
			x_range = np.arange(0, plot_df['wind_speed'].max()*1.1, 0.1)
			fig.add_trace(go.Scatter(x=x_range, y=self.slope * x_range + self.intercept, mode='lines', name=text, line=dict(color=colours[3], dash='dash')), row=2, col=1)
			fig.update_xaxes(title_text='Daily Mean Wind Speed (m/s)', row=2, col=1,titlefont=dict(size=12))
			fig.update_yaxes(title_text='Actual Daily Mean Generation (GWh) - Curtailed GWh', row=2, col=1,titlefont=dict(size=12))
			fig.update_yaxes(range=[0,plot_df[self.COL_DAILY_IDEAL_YIELD].max()*1.1], row=2, col=1)
			fig.update_xaxes(range=[0,plot_df['wind_speed'].max()*1.1], row=2, col=1)

			#subplot 3
			text = [f"Predicted: {pred:.2f} GWh<br>Actual: {actual:.2f} GWh<br>Month: {month}" for pred, actual, month in zip(plot_df[self.COL_DAILY_PREDICTED], plot_df[self.COL_DAILY_IDEAL_YIELD], plot_df.index.strftime('%b %Y'))]
			fig.add_trace(go.Scatter(x=plot_df[self.COL_DAILY_PREDICTED], y=plot_df[self.COL_DAILY_IDEAL_YIELD], mode='markers',marker_color = colours[2],legendgroup='3',
										hovertext=text,hoverinfo='text', showlegend=False), row=3, col=1)
			_lin_reg_df = plot_df[[self.COL_DAILY_PREDICTED, self.COL_DAILY_IDEAL_YIELD]].dropna()
			# linear regression between predicted and actual
			slope, intercept, r_value, p_value, ideal_std_err = stats.linregress(_lin_reg_df[self.COL_DAILY_PREDICTED], _lin_reg_df[self.COL_DAILY_IDEAL_YIELD])
			x_range = np.arange(0, plot_df[self.COL_DAILY_PREDICTED].max()*1.1, 0.1)
			r2_text = f'R^2 = {r_value**2:.2f}'

			fig.add_trace(go.Scatter(x=x_range, y=slope * x_range + intercept, mode='lines', name=r2_text, line=dict(color='red', dash='dash')), row=3, col=1)
			fig.update_xaxes(title_text='Predicted Daily Mean Generation (GWh)', row=3, col=1 ,titlefont=dict(size=12))
			fig.update_yaxes(title_text='Actual Daily Mean Generation (GWh)', row=3, col=1,titlefont=dict(size=12))
			fig.update_yaxes(range=[0,plot_df[self.COL_DAILY_IDEAL_YIELD].max()*1.1], row=3, col=1)
			fig.update_xaxes(range=[0,plot_df[self.COL_DAILY_PREDICTED].max()*1.1], row=3, col=1)

			
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
			# make a folder for the plots
			plot_folder = os.path.join(project_root_path, 'plots')
			if not os.path.exists(plot_folder):
				os.mkdir(plot_folder)
			filename = f'3_{self.bmu}_p50'
			plot_path = os.path.join(plot_folder, filename)
			# fig = plt.figure(figsize=(10,5))
			# ax = fig.add_subplot(111)
			
			# ax.bar(self.energy_yield_df.index-0.15,width=0.3, height = self.energy_yield_df[self.COL_QCd_YIELD], label='Expected (without curtailment losses)')

			# ax.bar(self.energy_yield_df.index+0.15,width=0.3,height= self.energy_yield_df['net_yield_GWh'], label='Expected (with curtailment losses)')
			# # make the ticks 'Jan', 'Feb' as opposed to 1, 2
			# labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
			# ax.legend()
			# ax.set_xticks(self.energy_yield_df.index)
			# ax.set_xticklabels(labels)

			# ax.set_xlabel('Month')
			# ax.set_ylabel('P50 Energy Yield (GWh)')
			# # r2 text

			# fig.suptitle(f'{self.bmu}, Capacity {self.capacity:.0f}MW Annual P50 Energy Yield: {self.p50_energy_yield:.0f} GWh, number of months: {self.n_data_points}\nModel $r^2$: {self.prediction_r2:.2f}')

			# losses_text = f'Annual observed losses due to curtailment: {self.losses_due_to_curtailment:.0f} GWh {self.losses_as_percentage}'
			# ax.set_title(losses_text)

			# fig.savefig(f"{plot_path}.png")
			# plt.close()
			fig = go.Figure()
			fig.add_trace(go.Bar(x=self.energy_yield_df.index-0.15,width=0.3, y = self.energy_yield_df[self.COL_QCd_YIELD], name='Expected (without curtailment losses)'))
			fig.add_trace(go.Bar(x=self.energy_yield_df.index+0.15,width=0.3,y= self.energy_yield_df['net_yield_GWh'], name='Expected (with curtailment losses)'))
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


