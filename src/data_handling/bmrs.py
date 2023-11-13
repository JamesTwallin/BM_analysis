import concurrent.futures
import glob
import itertools
import requests
import pandas as pd
assert pd.__version__ >= '1.5'
import os, sys
import json
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

def get_out_of_date_status(last_date):
    """
    Returns True if the last date in the data is not yesterday, False otherwise.
    """
    yesterday = pd.to_datetime('today').floor('D') - pd.Timedelta(days=1)
    if last_date is None or last_date < yesterday:
        return True
    else:
        return False








def download_gen_data(date_list, bmu_id, redo=False):
    print(f"Downloading generation data for {bmu_id}")
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = list(tqdm.tqdm(executor.map(_get_gen_df, date_list, itertools.repeat(bmu_id), itertools.repeat(redo)), total=len(date_list)))
    return results


def _get_gen_df(date, bmu_id, redo=False, verbose=False):
    folder_path = os.path.join(project_root_path, 'data', 'raw_gen_data', bmu_id)
    os.makedirs(folder_path, exist_ok=True)
    date_string = date.strftime('%Y-%m-%d')
    filename = os.path.join(folder_path, f'{date_string}.parquet')
    empty_filename = os.path.join(folder_path, f'{date_string}_empty.txt')

    if not redo and os.path.exists(filename):
        if verbose:
            print(f"File exists: {filename}")
        return pd.read_parquet(filename)

    if not redo and os.path.exists(empty_filename):
        if verbose:
            print(f"No data for {date_string}")
        return empty_filename  # Return the empty file indicator

    try:
        df = _download_and_process_data(date_string, bmu_id, verbose)
        if df is not None and not df.empty:
            df.to_parquet(filename)
            if verbose:
                print(f"Saved {filename}")
            return df
        else:
            with open(empty_filename, 'w') as f:
                f.write('No data')
            return empty_filename
    except Exception as e:
        if verbose:
            print(f"Error at {date_string}: {e}")
        return None

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
        
def get_generation_data(bmu_id, update=False, redo=False):
    """
    Fetches the generation data for a given BMU ID. If update is True, it updates 
    the data with new information since the last recorded date. If there are no new files to process,
    returns the existing data or an empty DataFrame. Utilizes a data catalogue to track processed files.

    Args:
    bmu_id (str): The BMU ID to fetch the data for.
    update (bool): Whether to update the data.
    redo (bool): If True, reprocesses the data even if it has been processed before.

    Returns:
    DataFrame: The DataFrame containing the generation data.
    """
    folder_path = os.path.join(project_root_path, 'data', 'preprocessed_data', bmu_id)
    os.makedirs(folder_path, exist_ok=True)
    file_name = os.path.join(folder_path, f'{bmu_id}_generation_data.parquet')
    data_catalogue_filename = os.path.join(folder_path, f'{bmu_id}_catalogue.json')

    # Load or initialize the data catalogue
    if os.path.exists(data_catalogue_filename):
        with open(data_catalogue_filename, 'r') as f:
            data_catalog = json.load(f)
    else:
        data_catalog = {}
        update = True  # Force update if no data catalogue exists

    # Load existing data if available
    existing_df = pd.read_parquet(file_name) if os.path.exists(file_name) else None
    last_date = existing_df['Settlement Date'].max() if existing_df is not None else None

    if not update and existing_df is not None:
        return existing_df

    # Determine the date range for downloading new data
    start_date = last_date + pd.Timedelta(days=1) if last_date is not None else pd.to_datetime('2017-01-01')
    end_date = pd.to_datetime('today').floor('D') - pd.Timedelta(days=1)

    date_list = pd.date_range(start_date, end_date, freq='1D')
    new_dates = [date for date in date_list if str(date.date()) not in data_catalog or redo]

    if new_dates:
        results = download_gen_data(new_dates, bmu_id, redo)
        # Update data_catalog with new dates
        for date in new_dates:
            data_catalog[str(date.date())] = 'Processed'
        with open(data_catalogue_filename, 'w') as f:
            json.dump(data_catalog, f)

        # Process and combine new data
        data_frames = [df for df in results if isinstance(df, pd.DataFrame)]
        if data_frames:
            new_df = pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()
            new_df.set_index(pd.to_datetime(new_df['Settlement Date']) + pd.to_timedelta((new_df['SP'] - 1) * 30, unit='minute'), inplace=True)
            new_df.index.name = 'utc_time'
            new_df = new_df.resample('30T').last()
            updated_df = pd.concat([existing_df, new_df], ignore_index=True) if existing_df is not None else new_df
            updated_df.to_parquet(file_name)

            return updated_df
    else:
        print("No new processed data to save.")
        return existing_df if existing_df is not None else pd.DataFrame()

def download_all_BAV_OAV_data(start_date, end_date, redo=False):
    """
    Downloads Bid Accepted Volume (BAV) and Offer Accepted Volume (OAV) data for each day within a specified date range.
    
    Args:
    start_date (datetime): The start date for the data download.
    end_date (datetime): The end date for the data download.
    redo (bool): If True, re-download and process the data even if it already exists.

    Returns:
    list: A list of results from downloading the data.
    """
    print(f"Downloading BAV and OAV from {start_date} to {end_date}")
    date_range = pd.date_range(start_date, end_date, freq='1D')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(tqdm.tqdm(executor.map(_download_accepted_volumes, date_range, itertools.repeat(redo)), total=len(date_range)))
    return results

def _download_accepted_volumes(date, redo=False, verbose=False):
    """
    Downloads and processes BAV and OAV data for a specific date.

    Args:
    date (datetime): The date for which to download the data.
    redo (bool): If True, re-download the data even if it already exists.
    verbose (bool): If True, print detailed log messages.

    Returns:
    None
    """
    date_str = date.strftime('%Y-%m-%d')
    folder_path = os.path.join(project_root_path, 'data', 'bm_data')

    os.makedirs(folder_path, exist_ok=True)

    oav_filename = os.path.join(folder_path, f'{date_str}_OAV.parquet')
    bav_filename = os.path.join(folder_path, f'{date_str}_BAV.parquet')

    if os.path.exists(oav_filename) and os.path.exists(bav_filename) and not redo:
        if verbose:
            print(f"Files already exist: {oav_filename}, {bav_filename}")
        return

    if verbose:
        print(f"Downloading data for {date_str}")

    endpoint = f"https://api.bmreports.com/BMRS/DERBMDATA/v1?APIKey={api_key}&SettlementDate={date_str}&SettlementPeriod=*&BMUnitId=*&BMUnitType=*&LeadPartyName=*&NGCBMUnitName=*&ServiceType=csv"
    response = requests.get(endpoint)

    if response.status_code != 200:
        if verbose:
            print(f"Failed to download data for {date_str}, status code: {response.status_code}")
        return

    df = pd.DataFrame(response.text.splitlines())
    df = df[0].str.split(',', expand=True)
    df.drop(0, inplace=True)

    try:
        bav_df = df[df[0] == 'BAV'].copy()
        bav_df.rename(columns={0: 'HDR', 1: 'BMU_id', 2: 'Settlement Period', 18: 'Total'}, inplace=True)
        bav_df['date'] = date
        bav_df.drop(columns=list(range(3, 18)), inplace=True)
        bav_df.to_parquet(bav_filename, index=False)
    except Exception as e:
        if verbose:
            print(f"Error processing BAV data for {date_str}: {e}")

    try:
        oav_df = df[df[0] == 'OAV'].copy()
        oav_df.rename(columns={0: 'HDR', 1: 'BMU_id', 2: 'Settlement Period', 18: 'Total'}, inplace=True)
        oav_df['date'] = date
        oav_df.drop(columns=list(range(3, 18)), inplace=True)
        oav_df.to_parquet(oav_filename, index=False)
    except Exception as e:
        if verbose:
            print(f"Error processing OAV data for {date_str}: {e}")

def get_bmu_curtailment_data(curtailment_df, bmu_id):
    """
    Processes and filters curtailment data for a specific BMU ID and saves it as a parquet file.
    Loads the data from a file if it already exists.

    Args:
    curtailment_df (DataFrame): The DataFrame containing curtailment data.
    bmu_id (str): The BMU ID to filter the data.

    Returns:
    DataFrame: A DataFrame containing the filtered and processed curtailment data.
    """
    folder_path = os.path.join(project_root_path, 'data', 'preprocessed_data', bmu_id)
    os.makedirs(folder_path, exist_ok=True)
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
    Fetches and updates curtailment data. If update is False, returns the existing data;
    if True, updates the data up to yesterday.

    Args:
    update (bool): Determines whether to update the data.

    Returns:
    DataFrame: The DataFrame containing the curtailment data.
    """
    preprocessed_folder = os.path.join(project_root_path, 'data', 'preprocessed_data')
    os.makedirs(preprocessed_folder, exist_ok=True)
    filename = os.path.join(preprocessed_folder, 'curtailment_data.parquet')

    existing_df = None
    if os.path.exists(filename):
        existing_df = pd.read_parquet(filename)
        last_date = existing_df['date'].max()
        out_of_date = get_out_of_date_status(last_date)

    if not update or (existing_df is not None and not out_of_date):
        print("Data is up to date" if existing_df is not None and not out_of_date else "Returning existing data")
        return existing_df

    yesterday = pd.to_datetime('today').floor('D') - pd.Timedelta(days=1)
    start_date = last_date if existing_df is not None else pd.to_datetime('2017-01-01')

    download_all_BAV_OAV_data(start_date, yesterday)

    file_list = glob.glob(os.path.join(project_root_path, 'data', 'bm_data', '*BAV*.parquet'))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(pd.read_parquet, file_list))

    all_data = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
    all_data = all_data.astype({'HDR': str, 'BMU_id': str, 'Settlement Period': int, 'Total': float})
    all_data['date'] = pd.to_datetime(all_data['date'])

    all_data.to_parquet(filename, index=False)
    return all_data

# if __name__ == "__main__":

#     # fetch_all_curtailment_data(upcdate=True) 
#     for bmu in ['BRDUW-1', 'ABRBO-1']:
        
#         gen_df =  get_generation_data(bmu)
#         # what the datatypes
#         print(gen_df.dtypes)
#         # does it have an index?
#         print(gen_df.index)
