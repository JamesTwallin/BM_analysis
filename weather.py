import boto3
import botocore
import datetime as dt
import os.path
import xarray as xr
import pandas as pd
import concurrent.futures
import netCDF4



from helpers import load_windfarms_geojson


era5_bucket = 'era5-pds'
client = boto3.client('s3',config=botocore.client.Config(signature_version=botocore.UNSIGNED))


def get_era5_data(date, variable,locs):
    folder = 'weather_data'
    if not os.path.exists(folder):
      os.makedirs(folder)
    drive_filename = folder + '/' + str(date.year) + '_' + str(date.month).zfill(2) + '_' + variable + '.parquet'
    if os.path.isfile(drive_filename):
      print('file_exists')
    else:
      year = date.year
      month = str(date.month).zfill(2)
      var = variable
      # file path patterns for remote S3 objects and corresponding local file
      s3_data_ptrn = '{year}/{month}/data/{var}.nc'
      data_file_ptrn = '{year}{month}_{var}.nc'

      s3_data_key = s3_data_ptrn.format(year=year, month=month, var=var)
      data_file = data_file_ptrn.format(year=year, month=month, var=var)

      if not os.path.isfile(data_file): # check if file already exists
          print("Downloading %s from S3..." % s3_data_key)
          client.download_file(era5_bucket, s3_data_key, data_file)
      print("Reading and processing %s..." % data_file)
      ds = xr.open_dataset(data_file)
      all_df = pd.DataFrame()
      # print(locs)
      for location in locs:
        print(f"Processing {location['lon']}, {location['lat']}")
        df = ds.sel(lon=location['adj_lon'], lat=location['lat'], method='nearest').to_dataframe()
        # print(df.head())
        all_df = pd.concat([all_df, df])
      
      all_df.to_parquet(drive_filename)
      open(data_file, 'w').close()
      os.remove(data_file)
      print('file_downloaded')

if __name__ == "__main__":
    windfarm_details = load_windfarms_geojson()
    windfarm_details.dropna(subset=['capacity'], inplace=True)
    windfarm_details = windfarm_details.loc[~windfarm_details[['lon','lat']].duplicated()]
    locs = windfarm_details[['lon','lat']].to_dict('records')
    for l in locs:
        if l['lon'] < 0:
            l['adj_lon'] = 360 + l['lon']
        else:
            l['adj_lon'] = l['lon']
    variables = ['eastward_wind_at_100_metres','northward_wind_at_100_metres']
    arg_list = []
    today = dt.datetime.utcnow().strftime('%Y-%m-%d')
    month_range = pd.date_range('2017-01-01',today,freq='1MS')
    var_month = [(v,m) for v in variables for m in month_range]
    with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
      for variable, month in var_month:
          executor.submit(get_era5_data, date=month, variable=variable,locs=locs)