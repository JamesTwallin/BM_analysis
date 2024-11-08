import boto3
import botocore
import datetime as dt
import glob
import os
import xarray as xr
import pandas as pd
import concurrent.futures
import netCDF4
import cdsapi
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import utils.helpers as helpers
# Global variable for project root path
project_root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

era5_bucket = 'era5-pds'
client = boto3.client('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))

def get_era5_data(date, variable, locs):
    folder = os.path.join(project_root_path, 'data', 'weather_data')
    os.makedirs(folder, exist_ok=True)



    dataset = "reanalysis-era5-single-levels"
    request = {
        "product_type": ["reanalysis"],
        "variable": [
            "10m_u_component_of_wind",
            "10m_v_component_of_wind"
        ],
        "year": [f"{date.year}"],
        "month": [f"{date.month}"],
        "day": [ "01", "02", "03",
        "04", "05", "06",
        "07", "08", "09",
        "10", "11", "12",
        "13", "14", "15",
        "16", "17", "18",
        "19", "20", "21",
        "22", "23", "24",
        "25", "26", "27",
        "28", "29", "30",
        "31"],
        "time": [
            "00:00", "01:00", "02:00",
            "03:00", "04:00", "05:00",
            "06:00", "07:00", "08:00",
            "09:00", "10:00", "11:00",
            "12:00", "13:00", "14:00",
            "15:00", "16:00", "17:00",
            "18:00", "19:00", "20:00",
            "21:00", "22:00", "23:00"
        ],
        "data_format": "netcdf",
        "download_format": "zip",
        "area": [62, -6, 48, 5]
    }

    client = cdsapi.Client()
    client.retrieve(dataset, request).download(target=f"{date.year}{str(date.month).zfill(2)}_{variable}.zip")
    

    
    drive_filename = os.path.join(folder, f"{date.year}_{str(date.month).zfill(2)}_{variable}.parquet")

    if os.path.isfile(drive_filename):
        print('File already exists:', drive_filename)
        return

    s3_data_key = f"{date.year}/{str(date.month).zfill(2)}/data/{variable}.nc"
    data_file = f"{date.year}{str(date.month).zfill(2)}_{variable}.nc"

    local_file_path = os.path.join(folder, data_file)
    if not os.path.isfile(local_file_path):
        print("Downloading from S3:", s3_data_key)
        try:
            client.download_file(era5_bucket, s3_data_key, local_file_path)
        except botocore.exceptions.ClientError as e:
            raise e

    print("Reading and processing:", local_file_path)
    ds = xr.open_dataset(local_file_path)
    all_df = pd.DataFrame()

    for location in locs:
        print(f"Processing {location['lon']}, {location['lat']}")
        df = ds.sel(lon=location['adj_lon'], lat=location['lat'], method='nearest').to_dataframe()
        all_df = pd.concat([all_df, df])

    all_df.to_parquet(drive_filename)
    open(local_file_path, 'w').close()
    os.remove(local_file_path)
    print('File downloaded:', drive_filename)

def fetch_all_weather_data():
    preprocessed_folder = os.path.join(project_root_path, 'data', 'preprocessed_data')
    os.makedirs(preprocessed_folder, exist_ok=True)

    filename = os.path.join(preprocessed_folder, 'weather_data.parquet')

    if os.path.exists(filename):
        print('File exists:', filename)
        return pd.read_parquet(filename)

    file_list = glob.glob(os.path.join(project_root_path, 'data', 'weather_data', '*.parquet'))
    with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
        dfs = executor.map(pd.read_parquet, file_list)
    df = pd.concat(dfs)
    df.to_parquet(filename)
    return df


if __name__ == "__main__":

    windfarm_details = helpers.load_windfarms_geojson(get_type=False)
    windfarm_details.dropna(subset=['capacity'], inplace=True)
    # round the lat and lon to .01
    windfarm_details['lat'] = windfarm_details['lat'].round(2)
    windfarm_details['lon'] = windfarm_details['lon'].round(2)
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
    month_range = pd.date_range('2024-08-01',today,freq='1MS')
    var_month = [(v,m) for v in variables for m in month_range]
    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
      for variable, month in var_month:
          executor.submit(get_era5_data, date=month, variable=variable,locs=locs)