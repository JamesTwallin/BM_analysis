import pandas as pd
import numpy as np
from statistics import linear_regression, correlation

# random forest regressor
from sklearn.ensemble import RandomForestRegressor
# test train split
from sklearn.model_selection import train_test_split
import pickle


import os

import matplotlib.pyplot as plt
from utils.helpers import get_nearest_lat_lon


### plotting stuff



# change the matplotlib font to use open sans
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = 'Open Sans'

COLOURS = ['#3F7D20', '#A0DDE6', '#542e71','#3F7CAC','#698F3F']

global project_root_path
project_root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def load_model(bmu, X_train, y_train):
	os.makedirs(os.path.join(project_root_path, 'models'), exist_ok=True)
	try:
		# make a folder for the plots
		model_folder = os.path.join(project_root_path, 'models')
		if not os.path.exists(model_folder):
			os.mkdir(model_folder)
		filename = f'{bmu}_model.pkl'
		model_path = os.path.join(model_folder, filename)
		with open(model_path, 'rb') as f:
			model = pickle.load(f)
		return model
	except Exception as e:
		model = RandomForestRegressor()
		model.fit(X_train, y_train)
		with open(model_path, 'wb') as f:
			pickle.dump(model, f)
		return model


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
		prediction_file_path = os.path.join(project_root_path, 'data', 'predictions', f'{self.bmu}.parquet')
		os.makedirs(os.path.dirname(prediction_file_path), exist_ok=True)
		# unseen data
		unseen_file_path = os.path.join(project_root_path, 'data', 'unseen_data', f'{self.bmu}.parquet')
		os.makedirs(os.path.dirname(unseen_file_path), exist_ok=True)


		if os.path.exists(prediction_file_path):
			self.preprocessed_df = pd.read_parquet(prediction_file_path)
			self.prediction_ok = True
			return 
		try:
			if self.preprocessed_df is None:
				self._preprocess_data()
			# test train split
			ml_df = self.preprocessed_df[['wind_speed', 'wind_direction_degrees', self.COL_IDEAL_YIELD]].dropna().copy()
			# round the wind direction to the nearest 5
			ml_df['wind_direction_degrees'] = ml_df['wind_direction_degrees'].apply(lambda x: round(x/5)*5)
			# round the wind speed to the nearest 0.5
			ml_df['wind_speed'] = ml_df['wind_speed'].apply(lambda x: round(x*2)/2)
			# drop any duplicates
			filt = ml_df.duplicated(subset=['wind_speed', 'wind_direction_degrees', self.COL_IDEAL_YIELD], keep='first')

			# ideal yield = 0
			filt2 = ml_df[self.COL_IDEAL_YIELD] == 0
			# count the number of duplicates
			n_duplicates = filt.sum()
			print('n_duplicates: ', n_duplicates)
			# drop the duplicates and where the ideal yield is 0
			ml_df = ml_df[~filt & ~filt2]
			# any values where the ideal yield is 0, drop them
			ml_df = ml_df[ml_df[self.COL_IDEAL_YIELD] != 0]
			# resample to the median for the day
			X = ml_df[['wind_speed', 'wind_direction_degrees']]

			y = ml_df[self.COL_IDEAL_YIELD]
			X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
			# fit the model
			model = load_model(self.bmu, X_train, y_train)
			# get the predictions
			y_pred = model.predict(X_test)
			self.preprocessed_df[self.COL_PREDICTED_IDEAL_YIELD] = np.nan
			# where the feature cols are not null, predict the ideal yield
			filt = self.preprocessed_df[['wind_speed', 'wind_direction_degrees']].notnull().all(axis=1)
			self.preprocessed_df.loc[filt, self.COL_PREDICTED_IDEAL_YIELD] = model.predict(self.preprocessed_df.loc[filt, ['wind_speed', 'wind_direction_degrees']])# 
			self.prediction_ok = True
			# save the file to the data folder
			self.preprocessed_df.to_parquet(prediction_file_path)


		except Exception as e:
			print('get_ml_prediction(), error: ', e)
			self.prediction_ok = False
		

	


	def _preprocess_data(self):
		# merge bav, gen and weather data
		
		merged_df= self.ws_df[['wind_speed']].resample('30T').last().copy()
		merged_df['wind_direction_degrees'] = self.ws_df[['wind_direction_degrees']].resample('30T').last().copy()
		merged_df['Quantity (MW)'] = self.gen_df[['Quantity (MW)']].resample('30T').last().copy()
		day_gen_df = self.gen_df[['Quantity (MW)']].resample('1D').count()
		# merge this and forward fill
		merged_df['gen_count'] = day_gen_df.reindex(merged_df.index, method='ffill').copy()
		merged_df['gen_count_bool'] = merged_df['gen_count'] >= 48.
		
		

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


	def auto_qc(self, month_df):
    # Initialize columns
		for col in [self.COL_NET_YIELD, self.COL_IDEAL_YIELD]:
			month_df[col+'_ok'] = np.nan
			month_df[col+'_fail'] = np.nan

		# Apply filters
		filt = (month_df['data_coverage_%'] >= 60) & (month_df['running_well'])
		actual_data_filt = month_df['peak_generation'].notnull()

		month_df.loc[filt, self.COL_NET_YIELD+'_ok'] = month_df.loc[filt, self.COL_NET_YIELD]
		month_df.loc[filt, self.COL_IDEAL_YIELD+'_ok'] = month_df.loc[filt, self.COL_IDEAL_YIELD]
		month_df.loc[~filt, self.COL_NET_YIELD+'_fail'] = month_df.loc[~filt & actual_data_filt, self.COL_NET_YIELD]
		month_df.loc[~filt, self.COL_IDEAL_YIELD+'_fail'] = month_df.loc[~filt & actual_data_filt, self.COL_IDEAL_YIELD]

		# More QC calculations
		month_df[self.COL_QCd_YIELD] = month_df[self.COL_IDEAL_YIELD+'_ok']
		month_df.loc[month_df[self.COL_IDEAL_YIELD+'_ok'].isnull(), self.COL_QCd_YIELD] = month_df.loc[month_df[self.COL_IDEAL_YIELD+'_ok'].isnull(), self.COL_PREDICTED_IDEAL_YIELD]

		# Divide by the number of days in the month for daily calculations
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

	def calculate_monthly_df(self):
		if ~self.prediction_ok:
			self.get_ml_prediction()
		_df = self.preprocessed_df.copy()
		# Group by month
		month_df = _df[[self.COL_NET_YIELD, self.COL_IDEAL_YIELD, self.COL_CURTAILMENT_LOSSES, self.COL_PREDICTED_IDEAL_YIELD]].resample('1MS').sum()

		# Calculate the availability for each month
		month_df['data_coverage_%'] = _df[self.COL_NET_YIELD].resample('1MS').count() / (month_df.index.days_in_month * 48) * 100.
		month_df['peak_generation'] = _df[self.COL_NET_YIELD].resample('1MS').max()

		# Get the 90th percentile generation
		running_well_val = _df[self.COL_NET_YIELD].quantile(0.85)
		month_df['running_well'] = month_df['peak_generation'] >= running_well_val

		return month_df



	def load_month_df(self):
		file_path = os.path.join(project_root_path, 'data', 'pcey_data', f'{self.bmu}_monthly.parquet')
		os.makedirs(os.path.dirname(file_path), exist_ok=True)
		if os.path.exists(file_path):
			self.month_df = pd.read_parquet(file_path)
		else:
       	 	# Step 1: Calculate the monthly data frame
			monthly_df = self.calculate_monthly_df()
        	# Step 2: Apply auto quality control
			self.auto_qc(monthly_df)
			# save the file
			self.month_df.to_parquet(file_path)


	def _load_fit_dict(self, df):
		'''
		uses stats.linregress to calculate the r2
		compares the predicted ideal yield with the actual ideal yield
		this is the _ok column
		'''
		# drop the na values
		_lin_reg_df = df[[self.COL_DAILY_PREDICTED, self.COL_DAILY_IDEAL_YIELD+'_ok']].dropna()
		# linear regression between predicted and actual
		slope, intercept = linear_regression(_lin_reg_df[self.COL_DAILY_PREDICTED], _lin_reg_df[self.COL_DAILY_IDEAL_YIELD+'_ok'])
		r_value = correlation(_lin_reg_df[self.COL_DAILY_PREDICTED], _lin_reg_df[self.COL_DAILY_IDEAL_YIELD+'_ok'])
		self.fit_dict = {'slope': slope, 'intercept': intercept, 'r_value': r_value}
		self.prediction_r2 = r_value**2


	def _calculate_monthly_curtailment_losses(self,df):
		'''
		This takes in a dataframe with the following columns:
		- ideal yield
		- measured losses
		for the total range, this function calculates the monthly curtailment losses
		as a percentage of the ideal yield for each month
		'''
		# group by month
		month_df = df[[self.COL_IDEAL_YIELD, self.COL_CURTAILMENT_LOSSES]].copy()
		# group by month and get the median
		# calculate the curtailment losses as a percentage of the ideal yield
		month_df['curtailment_losses_%'] = month_df[self.COL_CURTAILMENT_LOSSES] / month_df[self.COL_IDEAL_YIELD] * 100.
		# group by month number and get the median
		month_df['month_number'] = month_df.index.month
		month_df = month_df[['curtailment_losses_%', 'month_number']].groupby('month_number').median()
		return month_df['curtailment_losses_%']
	
	def _calculate_monthly_energy_yield(self, df):
		'''
		This takes in a dataframe with the following columns:
		- ideal yield
		- measured losses
		for the total range, this function calculates the monthly curtailment losses
		as a percentage of the ideal yield for each month
		'''
		# group by month
		month_df = df[[self.COL_QCd_YIELD]].copy()

		# group by month number and get the median
		month_df['month_number'] = month_df.index.month

		month_df = month_df[[self.COL_QCd_YIELD, 'month_number']].groupby('month_number').median()

		return month_df[self.COL_QCd_YIELD]


	def calculate_energy_yield(self):
		if self.month_df is None:
			self.load_month_df()
		try:
		# merge the two dfs




			combined_df = pd.merge(self.weather_stats_df, self.month_df, left_index=True, right_index=True, how='outer')

			curtailment_losses = self._calculate_monthly_curtailment_losses(combined_df)
			energy_yield = self._calculate_monthly_energy_yield(combined_df)
			self._load_fit_dict(combined_df)
			# now add the percentage curtailment losses
			year_df = pd.DataFrame(index = [1,2,3,4,5,6,7,8,9,10,11,12])
			year_df['curtailment_losses_%'] = curtailment_losses
			year_df['energy_without_curtailment'] = energy_yield
			year_df['energy_with_curtailment'] = (1- curtailment_losses/100.) * energy_yield
			year_df['curtailment_losses'] = (curtailment_losses/100.) * energy_yield


			year_df['month_number'] = year_df.index
			# add the month name
			year_df['month_name'] = year_df['month_number'].apply(lambda x: pd.to_datetime(str(x), format='%m').strftime('%B'))
			# calculate the ideal yield
			self.p50_energy_yield = year_df['energy_with_curtailment'].sum()
			self.p50_ideal_yield = year_df['energy_without_curtailment'].sum()
			self.losses_due_to_curtailment = year_df['curtailment_losses'].sum()
			self.losses_as_percentage = self.losses_due_to_curtailment / self.p50_energy_yield * 100.
			self.n_data_points = self.month_df[self.COL_NET_YIELD+'_ok'].count()
			self.energy_yield_df = year_df.copy()

		except Exception as e:
			print('calculate_energy_yield(), error: ', e)

