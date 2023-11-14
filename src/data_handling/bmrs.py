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
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = list(tqdm.tqdm(executor.map(_get_gen_df, date_list, itertools.repeat(bmu_id), itertools.repeat(redo)), total=len(date_list)))
    return results

def _get_gen_df(date_string, bmu_id, redo=False, verbose=False):
    if isinstance(date_string, pd.Timestamp):
        date_string = date_string.strftime('%Y-%m-%d')
    folder_path = os.path.join(project_root_path, 'data', 'raw_gen_data', bmu_id)
    os.makedirs(folder_path, exist_ok=True)
    filename = os.path.join(folder_path, f'{date_string}.parquet')
    empty_filename = os.path.join(folder_path, f'{date_string}_empty.txt')

    if not redo and os.path.exists(filename):
        if verbose:
            print(f"File exists: {filename}")
        return pd.read_parquet(filename)

    if not redo and os.path.exists(empty_filename):
        if verbose:
            print(f"No data for {date_string}")
        return None  # Return None to indicate no new data

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
            return None
    except Exception as e:
        if verbose:
            print(f"Error processing data for {date_string}: {e}")
        return None

def _download_and_process_data(date_string, bmu_id, verbose):
    endpoint = f"https://api.bmreports.com/BMRS/B1610/v2?APIKey={api_key}&SettlementDate={date_string}&Period=*&NGCBMUnitID={bmu_id}&ServiceType=csv"
    try:
        response = requests.get(endpoint)
    except requests.exceptions.HTTPError as e:
        if verbose:
            print(f"HTTP error: {e}")
        return None
    except Exception as e:
        if verbose:
            print(f"Error fetching data: {e}")
        return None

    df = pd.DataFrame(response.text.splitlines())
    try:
        df = df[0].str.split(',', expand=True).drop(0).rename(columns=df.iloc[0]).drop(1)
        df = df.astype({'SP': 'int', 'Quantity (MW)': 'float'})
        df['Settlement Date'] = pd.to_datetime(df['Settlement Date'])
        return df[['Settlement Date', 'SP', 'Quantity (MW)']]
    except Exception as e:
        if verbose:
            print(f"Error processing data: {e}")
        return None

def get_generation_data(bmu_id, update=False, redo=False):
    folder_path = os.path.join(project_root_path, 'data', 'preprocessed_data', bmu_id)
    os.makedirs(folder_path, exist_ok=True)
    file_name = os.path.join(folder_path, f'{bmu_id}_generation_data.parquet')
    data_catalogue_filename = os.path.join(folder_path, f'{bmu_id}_catalogue.json')

    data_catalog = {}
    if os.path.exists(data_catalogue_filename):
        with open(data_catalogue_filename, 'r') as f:
            data_catalog = json.load(f)
    else:
        data_catalog['attempted'] = {}
        data_catalog['processed'] = {}
        update = True

    # glob the raw_gen_data folder to get all the dates that have been attempted
    attempted_dates = glob.glob(os.path.join(project_root_path, 'data', 'raw_gen_data', bmu_id, "*"))
    # take the first 10 characters of the filename to get the date
    attempted_dates = [os.path.basename(date)[:10] for date in attempted_dates]
    for _date in attempted_dates:
        data_catalog['attempted'][_date] = True
    if os.path.exists(file_name):

        existing_df = pd.read_parquet(file_name) 
        last_date = existing_df['Settlement Date'].max()
    
    else:
        # Use a ThreadPoolExecutor to read parquet files in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            files = glob.glob(os.path.join(project_root_path, 'data', 'raw_gen_data', bmu_id, "*.parquet"))
            if files:  # Check if there are any files to process
                future_to_parquet = {executor.submit(pd.read_parquet, file): file for file in files}
                results = []
                for future in concurrent.futures.as_completed(future_to_parquet):
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as exc:
                        print('%r generated an exception: %s' % (future_to_parquet[future], exc))
                
                existing_df = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
                last_date = existing_df['Settlement Date'].max()
                existing_df.to_parquet(file_name)

            else:
                existing_df = pd.DataFrame()
                last_date = None


    if not update and not existing_df.empty:
        with open(data_catalogue_filename, 'w') as f:
            json.dump(data_catalog, f)
        return existing_df

    start_date = last_date + pd.Timedelta(days=1) if last_date else pd.to_datetime('2017-01-01')
    end_date = pd.to_datetime('today').floor('D') - pd.Timedelta(days=1)
    date_list = pd.date_range(start_date, end_date, freq='1D')
    new_dates = [date for date in date_list if date.strftime('%Y-%m-%d') not in data_catalog['attempted']]

    if new_dates:
        print(f"Downloading data for {bmu_id} from {new_dates[0]} to {new_dates[-1]}")
        results = download_gen_data(new_dates, bmu_id, redo)
        data_frames = [df for df in results if isinstance(df, pd.DataFrame)]
        if data_frames:
            new_df = pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()
            new_df.set_index(pd.to_datetime(new_df['Settlement Date']) + pd.to_timedelta((new_df['SP'] - 1) * 30, unit='minute'), inplace=True)
            new_df.index.name = 'utc_time'
            new_df = new_df.resample('30T').last()
            updated_df = pd.concat([existing_df, new_df], ignore_index=True) if not existing_df.empty else new_df
            updated_df.to_parquet(file_name)
            for df in data_frames:
                date_string = df['Settlement Date'].iloc[0].strftime('%Y-%m-%d')
                data_catalog['processed'][date_string] = True
        with open(data_catalogue_filename, 'w') as f:
            json.dump(data_catalog, f)
    else:

        print(f"{bmu_id} is up to date")


    return existing_df if not existing_df.empty else pd.DataFrame()

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


class BMU:
    def __init__(self, bmu_id, update_gen_data=False):
        self.bmu_id = bmu_id
        self.raw_folder_path = os.path.join(project_root_path, 'data', 'raw_gen_data', self.bmu_id)
        self.preprocessed_folder_path = os.path.join(project_root_path, 'data', 'preprocessed_data', self.bmu_id)
        self.update_gen_data = update_gen_data
        self._load_metadata_dict()
        self.session = requests.Session()
        

    def _load_metadata_dict(self):
        metadata_file = os.path.join(self.raw_folder_path, f'{self.bmu_id}_metadata.json')
        if os.path.exists(metadata_file):
            with open(metadata_file, 'r') as f:
                metadata_dict = json.load(f)
            self.metadata_dict = metadata_dict
        else:
            self.metadata_dict = {}
            self.metadata_dict['attempted'] = {}
            self.metadata_dict['processed'] = {}
            self.update_gen_data = True

    def _update_metadata_dict(self):
        metadata_file = os.path.join(self.raw_folder_path, f'{self.bmu_id}_metadata.json')
        with open(metadata_file, 'w') as f:
            json.dump(self.metadata_dict, f)


    def _get_gen_data(self):
        try:
            start_date = pd.to_datetime('2017-01-01')
            end_date = pd.to_datetime('today').floor('D') - pd.Timedelta(days=1)
            date_list = pd.date_range(start_date, end_date, freq='1D')
            new_dates = [date for date in date_list if date.strftime('%Y-%m-%d') not in self.metadata_dict['attempted']]
            if new_dates:
                # do chunks of 250 dates at a time
                chunks = [new_dates[i:i + 250] for i in range(0, len(new_dates), 250)]
                for chunk in chunks:
                # Use ThreadPoolExecutor to call API concurrently
                    
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        futures = {executor.submit(self._call_api, date.strftime('%Y-%m-%d')): date for date in chunk}
                        for future in concurrent.futures.as_completed(futures):
                            date = futures[future]
                            self.metadata_dict['attempted'][date.strftime('%Y-%m-%d')] = True
                            if future.result():
                                self.metadata_dict['processed'][date.strftime('%Y-%m-%d')] = True
                            else:
                                self.metadata_dict['processed'][date.strftime('%Y-%m-%d')] = False


                    self._update_metadata_dict()
            else:
                print(f"{self.bmu_id} is up to date")
        except Exception as e:
            print(f"Error processing data for {self.bmu_id}: {e}")

    def _call_api(self, date_string):
        endpoint = f"https://api.bmreports.com/BMRS/B1610/v2?APIKey={api_key}&SettlementDate={date_string}&Period=*&NGCBMUnitID={self.bmu_id}&ServiceType=csv"
        try:
            print(f"Downloading data for {date_string}")
            response = self.session.get(endpoint)
            df = pd.DataFrame(response.text.splitlines())
            df = df[0].str.split(',', expand=True)
            df.columns = df.iloc[1]
            df.drop([0, 1], inplace=True)

            df = df.astype({'SP': 'int', 'Quantity (MW)': 'float'})
            df['Settlement Date'] = pd.to_datetime(df['Settlement Date'])
            df[['Settlement Date', 'SP', 'Quantity (MW)']].to_parquet(f"{self.raw_folder_path}/{date_string}.parquet")
            return True
        except Exception as e:
            return False



            



if __name__ == "__main__":
    bmu_obj = BMU('BTUIW-2')
    bmu_obj._get_gen_data()