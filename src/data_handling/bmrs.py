import concurrent.futures
import glob
import itertools
import requests
import pandas as pd
assert pd.__version__ >= '1.5'
import os, sys
import ast
import tqdm

import src.utils.helpers as helpers
global api_key
api_key = helpers.get_credentials()

global project_root_path
project_root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class NoDataError(Exception):
    pass

class APIError(Exception):
    pass


def download_all_BAV_OAV_data(start_date, end_date, redo=False):
    print(f"BMRS DATA: Downloading BAV and OAV from {start_date} to {end_date}")
    date_range = pd.date_range(start_date, end_date, freq='1D')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # tdqm
        results = list(tqdm.tqdm(executor.map(_download_accepted_volumes, date_range, itertools.repeat(redo)), total=len(date_range)))
    return results



def download_gen_data(start, end, bmu_id, redo=False):
    print(f"Downloading generation data for {bmu_id} from {start} to {end}")
    date_range = pd.date_range(start, end, freq='1D')
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = list(tqdm.tqdm(executor.map(_get_gen_df, date_range, itertools.repeat(bmu_id), itertools.repeat(redo)), total=len(date_range)))
    return results

def _get_gen_df(date, bmu_id, redo=False, verbose=False):
    folder_path = os.path.join(project_root_path, 'data', 'raw_gen_data', bmu_id)
    os.makedirs(folder_path, exist_ok=True)
    date_string = date.strftime('%Y-%m-%d')
    filename = os.path.join(folder_path, f'{date_string}.parquet')

    # Check if file exists or if there is a flag indicating no data for this date
    if _check_file_status(filename, redo, verbose, date_string):
        return

    try:
        df = _download_and_process_data(date_string, bmu_id, verbose)
        if df is not None and not df.empty:
            df.to_parquet(filename)
            if verbose:
                print(f"Saved {filename}")
    except Exception as e:
        _handle_download_error(e, folder_path, date_string, verbose)

def _check_file_status(filename, redo, verbose, date_string):
    """
    Check if the file exists or if an empty flag file exists.
    Return True if processing should be skipped, False otherwise.
    """
    empty_filename = filename.replace('.parquet', '_empty.txt')
    if os.path.exists(filename) and not redo:
        if verbose:
            print(f"File exists: {filename}")
        return True
    if os.path.exists(empty_filename) and not redo:
        if verbose:
            print(f"No data for date: {date_string}")
        return True
    return False

def _download_and_process_data(date_string, bmu_id, verbose):
    """
    Download and process data for a specific date and bmu_id.
    """
    endpoint = f"https://api.bmreports.com/BMRS/B1610/v2?APIKey={api_key}&SettlementDate={date_string}&Period=*&NGCBMUnitID={bmu_id}&ServiceType=csv"
    response = requests.get(endpoint)
    if response.status_code != 200:
        if verbose:
            print(f"Failed to download data for {date_string}, status code: {response.status_code}")
        return None

    df = pd.DataFrame(response.text.splitlines())
    if df.empty:
        if verbose:
            print(f"No data for {date_string}")
        return None

    df = df[0].str.split(',', expand=True).drop(0).rename(columns=df.iloc[0]).drop(1)
    df = df.astype({'SP': 'int', 'Quantity (MW)': 'float'})
    df['Settlement Date'] = pd.to_datetime(df['Settlement Date'])
    return df[['Settlement Date', 'SP', 'Quantity (MW)']]

def _handle_download_error(e, folder_path, date_string, verbose):
    """
    Handle errors during data download and processing.
    """
    empty_filename = os.path.join(folder_path, f'{date_string}_empty.txt')
    with open(empty_filename, 'w') as f:
        f.write('No data')
    if verbose:
        print(f"Error at {date_string}, {e}, Line: {sys.exc_info()[2].tb_lineno}")
        
def get_generation_data(bmu_id, update=False):
    """
    Fetches the generation data for a given BMU ID. If update is True, it updates 
    the data with new information since the last recorded date. If there are no new files to process,
    returns the existing data or an empty DataFrame.
    """
    folder_path = os.path.join(project_root_path, 'data', 'preprocessed_data', bmu_id)
    os.makedirs(folder_path, exist_ok=True)
    file_name = os.path.join(folder_path, f'{bmu_id}_generation_data.parquet')

    # Initialize variables
    existing_df = None
    last_date = None

    # Check if the data file exists
    if os.path.exists(file_name):
        existing_df = pd.read_parquet(file_name)
        print(f"File exists: {file_name}")
        last_date = existing_df['Settlement Date'].max()

    # Return existing data if not updating
    if not update:
        return existing_df if existing_df is not None else pd.DataFrame()

    # Define the range for updating data
    start_date = last_date + pd.Timedelta(days=1) if last_date is not None else pd.to_datetime('2017-01-01')
    end_date = pd.to_datetime('today').floor('D') - pd.Timedelta(days=1)

    # Download new data
    download_gen_data(start_date, end_date, bmu_id)

    # Check for new files and aggregate new data
    new_file_list = [os.path.join(project_root_path, 'data', 'raw_gen_data', bmu_id, f"{date.strftime('%Y-%m-%d')}.parquet") for date in pd.date_range(start_date, end_date, freq='1D')]
    new_file_list = [file for file in new_file_list if os.path.exists(file)]
    if not new_file_list:
        print("No new data to process.")
        return existing_df if existing_df is not None else pd.DataFrame()

    new_df = pd.concat([pd.read_parquet(file) for file in new_file_list], ignore_index=True)

    # Process and format the new data
    if not new_df.empty:
        # Data processing steps (if any) can be added here

        # Combine new data with existing data
        updated_df = pd.concat([existing_df, new_df], ignore_index=True) if existing_df is not None else new_df

        # Save the updated data
        updated_df.to_parquet(file_name)
        return updated_df
    else:
        print("No new processed data to save.")
        return existing_df if existing_df is not None else pd.DataFrame()

def _download_accepted_volumes(date,redo=False,verbose=False):
    date_str = date.strftime('%Y-%m-%d')
    folder_path = os.path.join(project_root_path,'data', 'bm_data')

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    oav_filename = os.path.join(folder_path, f'{date_str}_OAV.parquet')
    bav_filename = os.path.join(folder_path, f'{date_str}_BAV.parquet')

    if os.path.exists(oav_filename) and os.path.exists(bav_filename) and not redo:
        if verbose:
            print(f"Files already exist: {oav_filename}, {bav_filename}")
        return None
    if verbose:
        print(f"Downloading data for {date_str}")
    endpoint = f"https://api.bmreports.com/BMRS/DERBMDATA/v1?APIKey={api_key}&SettlementDate={date_str}&SettlementPeriod=*&BMUnitId=*&BMUnitType=*&LeadPartyName=*&NGCBMUnitName=*&ServiceType=csv"
    response = requests.get(endpoint)

    if response.status_code != 200:
        if verbose:
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
        if verbose:
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
        if verbose:
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
  
def fetch_all_curtailment_data(update=False):
    """
    Fetches and optionally updates the curtailment data. If update is False, 
    it returns the existing data; if True, it updates the data up to yesterday.
    """
    # Set up the folder for preprocessed data
    preprocessed_folder = os.path.join(project_root_path, 'data', 'preprocessed_data')
    os.makedirs(preprocessed_folder, exist_ok=True)

    # Define the filename for curtailment data
    filename = os.path.join(preprocessed_folder, 'curtailment_data.parquet')

    # Initialize variables
    existing_df = None
    out_of_date = True

    # Check if the data file exists and determine if it's out of date
    if os.path.exists(filename):
        existing_df = pd.read_parquet(filename)
        last_date = existing_df['date'].max()
        out_of_date = get_out_of_date_status(last_date)

    # Return existing data if not updating or if data is already up to date
    if not update or (update and not out_of_date):
        if existing_df is not None:
            print("fetch_all_curtailment_data(): Data is up to date" if not out_of_date else "fetch_all_curtailment_data(): Returning existing data")
            return existing_df
        else:
            print("fetch_all_curtailment_data(): No existing data found. Fetching new data...")

    # Determine the range for updating data
    yesterday = pd.to_datetime('today').floor('D') - pd.Timedelta(days=1)
    start_date = last_date if existing_df is not None else pd.to_datetime('2017-01-01')

    # Download new data
    download_all_BAV_OAV_data(start_date, yesterday)

    # Aggregate new data from multiple files
    file_list = glob.glob(os.path.join(project_root_path, 'data', 'bm_data', '*BAV*.parquet'))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(pd.read_parquet, file_list))

    # Combine new data into a single DataFrame
    all_data = pd.concat(results, ignore_index=True)
    all_data = all_data.astype({'HDR': str, 'BMU_id': str, 'Settlement Period': int, 'Total': float})
    all_data['date'] = pd.to_datetime(all_data['date'])


    # Save the updated data
    all_data.to_parquet(filename, index=False)
    return all_data
    

if __name__ == "__main__":

    # fetch_all_curtailment_data(upcdate=True)
    gen_df =  get_generation_data('BRDUW-1', update=True)
    print(gen_df)
