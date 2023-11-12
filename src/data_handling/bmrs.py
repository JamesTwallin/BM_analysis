import concurrent.futures
import glob
import itertools
import requests
import pandas as pd
assert pd.__version__ >= '1.5'
import os, sys


import src.utils.helpers as helpers
global api_key
api_key = helpers.get_credentials()

global project_root_path
project_root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class NoDataError(Exception):
    pass

class APIError(Exception):
    pass



def get_all_accepted_volumes(start_date, end_date):
    date_range = pd.date_range(start_date, end_date, freq='1D')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(_download_accepted_volumes, date_range))
    return results  # Collect and return results if needed


def download_gen_data(start, end, bmu_id, redo=False):
    date_range = pd.date_range(start, end, freq='1D')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(_get_gen_df, date_range, itertools.repeat(bmu_id), itertools.repeat(redo))



def _download_accepted_volumes(date):
    date_str = date.strftime('%Y-%m-%d')
    folder_path = os.path.join(project_root_path, 'bm_data')

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    oav_filename = os.path.join(folder_path, f'{date_str}_OAV.parquet')
    bav_filename = os.path.join(folder_path, f'{date_str}_BAV.parquet')

    if os.path.exists(oav_filename) and os.path.exists(bav_filename):
        print(f"Files already exist: {oav_filename}, {bav_filename}")
        return None

    print(f"Downloading data for {date_str}")
    endpoint = f"https://api.bmreports.com/BMRS/DERBMDATA/v1?APIKey={api_key}&SettlementDate={date_str}&SettlementPeriod=*&BMUnitId=*&BMUnitType=*&LeadPartyName=*&NGCBMUnitName=*&ServiceType=csv"
    response = requests.get(endpoint)

    if response.status_code != 200:
        print(f"Failed to download data for {date_str}, status code: {response.status_code}")
        return None

    #turn the response into a dataframe, the text has a ',' delimiter
    df = pd.DataFrame(response.text.splitlines())
    # make the first row the column names
    
    # split ',' delimited columns into separate columns
    df = df[0].str.split(',', expand=True)
    # drop the first row
    df.drop(0, inplace=True)
    
    try:
        bav_df = df[df[0] == 'BAV'].copy()
        bav_df.rename(columns={0: 'HDR',1: 'BMU_id',2:'Settlement Period', 18: 'Total'}, inplace=True)

        bav_df['date'] = date

        bav_df.drop(columns=[3,4,5,6,7,8,9,10,11,12,13,14,15,16,17], inplace=True)

        bav_df.to_parquet(bav_filename, index=False)
    except Exception as e:
        print(f"Error: {e}, date: {date}")
        return None

    # process the OAV data:
    try:
        oav_df = df[df[0] == 'OAV'].copy()
        oav_df.rename(columns={0: 'HDR',1: 'BMU_id',2:'Settlement Period', 18: 'Total'}, inplace=True)

        oav_df['date'] = date

        oav_df.drop(columns=[3,4,5,6,7,8,9,10,11,12,13,14,15,16,17], inplace=True)

        oav_df.to_parquet(oav_filename, index=False)
    except Exception as e:
        print(f"Error: {e}, date: {date}")
        return None
        
    
def get_bmu_curtailment_data(curtailment_df, bmu_id):

    folder_path = os.path.join(project_root_path, 'data', 'preprocessed_data', bmu_id)

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    filename = os.path.join(folder_path, f'{bmu_id}_curtailment_data.parquet')

    if os.path.exists(filename):
        print('Loading from file')
        return pd.read_parquet(filename)

    filt = curtailment_df['BMU_id'].str.contains(bmu_id)
    df = curtailment_df[filt].copy()
    df['Settlement Period'] = df['Settlement Period'].astype(int)
    df.index = pd.to_datetime(df.pop('date'))
    df['utc_time'] = df.index + pd.to_timedelta((df['Settlement Period'] - 1) * 30, unit='minute')
    df.set_index('utc_time', inplace=True)
    df = df.resample('30T').last()
    df['Total'] = df['Total'].astype(float)
    df.to_parquet(filename)
    return df
  
def fetch_all_curtailment_data():

    preprocessed_folder = os.path.join(project_root_path, 'data', 'preprocessed_data')

    if not os.path.exists(preprocessed_folder):
        os.makedirs(preprocessed_folder)

    filename = os.path.join(preprocessed_folder, 'curtailment_data.parquet')

    if os.path.exists(filename):
        print(f"Loading curtailment data from file: {filename}")
        return pd.read_parquet(filename)

    file_list = glob.glob(os.path.join(project_root_path, 'data', 'bm_data', '*BAV*.parquet'))
    df = pd.DataFrame()

    for file in file_list:
        df = pd.concat([df, pd.read_parquet(file)], ignore_index=True)

    df.to_parquet(filename)
    return df




def _get_gen_df(date, bmu_id, redo=False):

    folder_path = os.path.join(project_root_path, 'data', 'raw_gen_data', bmu_id)
    os.makedirs(folder_path, exist_ok=True)

    date_string = date.strftime('%Y-%m-%d')
    filename = os.path.join(folder_path, f'{date_string}.parquet')

    if os.path.exists(filename) and not redo:
        print(f"File exists: {filename}")
        return

    try:
        endpoint = f"https://api.bmreports.com/BMRS/B1610/v2?APIKey={api_key}&SettlementDate={date_string}&Period=*&NGCBMUnitID={bmu_id}&ServiceType=csv"
        response = requests.get(endpoint)

        if response.status_code != 200:
            raise APIError(f"API call failed for {date_string}")
        df = pd.DataFrame(response.text.splitlines())

        # split ',' delimited columns into separate columns
        df = df[0].str.split(',', expand=True)
        # drop the first row
        df.drop(0, inplace=True)
        # make the first row the column names and drop the first row
        df.columns = df.iloc[0]
        df.drop(1, inplace=True)
        # assert that length is greater than 1
        if df.empty:
            raise NoDataError(f"No data for {bmu_id} on {date_string}")

        df['SP'] = df['SP'].astype('int')
        df['Quantity (MW)'] = df['Quantity (MW)'].astype('float')
        df['Settlement Date'] = pd.to_datetime(df['Settlement Date'])
        df = df[['Settlement Date', 'SP', 'Quantity (MW)']]

        df.to_parquet(filename)
        print(f"Saved {filename}")
    except (NoDataError, APIError) as e:
        print(e)
    except Exception as e:
        print(f"Error at {filename}, {e}, Line: {sys.exc_info()[2].tb_lineno}")


def get_generation_data(bmu_id):

    folder_path = os.path.join(project_root_path,'data', 'preprocessed_data', bmu_id)
    os.makedirs(folder_path, exist_ok=True)

    file_name = os.path.join(folder_path, f'{bmu_id}_generation_data.parquet')

    if os.path.exists(file_name):
        print(f"File exists: {file_name}")
        return pd.read_parquet(file_name)

    try:
        file_list = glob.glob(os.path.join(project_root_path,'data', 'raw_gen_data', bmu_id, '*.parquet'))
        df = pd.concat([pd.read_parquet(file) for file in file_list], ignore_index=True)

        df['SP'] = df['SP'].astype('int')
        df['Quantity (MW)'] = df['Quantity (MW)'].astype('float')
        df['Settlement Date'] = pd.to_datetime(df['Settlement Date'])
        

        df.set_index(pd.to_datetime(df['Settlement Date']) + pd.to_timedelta((df['SP'] - 1) * 30, unit='minute'), inplace=True)
        df.index.name = 'utc_time'
        df = df.resample('30T').last()

        df.to_parquet(file_name)
        return df
    except Exception as e:
        # raise  with the line number
        raise Exception(f"get_generation_data({bmu_id})Error at {file_name}, {e}, Line: {sys.exc_info()[2].tb_lineno}")


if __name__ == "__main__":
    get_generation_data('BEATO-3')