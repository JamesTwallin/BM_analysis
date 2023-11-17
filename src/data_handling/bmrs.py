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
import datetime as dt


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


    def _update_gen_data(self):
        try:
            start_date = pd.to_datetime('2017-01-01')
            end_date = pd.to_datetime('today').floor('D') - pd.Timedelta(days=1)
            date_list = pd.date_range(start_date, end_date, freq='1D').to_list()
            # get a list of dates that have not been processed True
            for date in date_list:
                #get the boolean value for the date
                date_string = date.strftime('%Y-%m-%d')
                processed = self.metadata_dict['processed'].get(date_string)
                if processed:
                    # if the date has been processed, remove it from the list
                    date_list.remove(date)

            # only keep more recent dates
            new_dates = [date for date in date_list if date > pd.to_datetime('today').floor('D') - pd.Timedelta(days=14)]
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
                    # every 250 dates, update the metadata file, to ensure that we don't re-run the same dates if the script crashes
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
            print(f"Error processing data for {date_string}: {e}")
            return False

    def _get_new_processed_dates(self, last_date):
        return [
            date for date in self.metadata_dict['processed']
            if self.metadata_dict['processed'][date] and pd.to_datetime(date) > last_date
        ]

    def _read_and_concatenate_dataframes(self, dates):
        file_paths = [os.path.join(self.raw_folder_path, f'{date}.parquet') for date in dates]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            dataframes = list(executor.map(pd.read_parquet, file_paths))
        return pd.concat(dataframes, ignore_index=True)

    def get_all_gen_data(self):
        if self.update_gen_data:
            self._update_gen_data()

        gen_data_file = os.path.join(self.preprocessed_folder_path, f'{self.bmu_id}_generation_data.parquet')
        if os.path.exists(gen_data_file):
            all_data = pd.read_parquet(gen_data_file)
            last_processed_date = all_data['Settlement Date'].max()

            new_dates = self._get_new_processed_dates(last_processed_date)
            if new_dates:
                new_data = self._read_and_concatenate_dataframes(new_dates)
                all_data = pd.concat([all_data, new_data], ignore_index=True)
                all_data.to_parquet(gen_data_file)

            return all_data

        metadata_file = os.path.join(self.raw_folder_path, f'{self.bmu_id}_metadata.json')
        with open(metadata_file, 'r') as file:
            metadata_dict = json.load(file)

        processed_dates = [date for date in metadata_dict['processed'] if metadata_dict['processed'][date]]
        all_data = self._read_and_concatenate_dataframes(processed_dates)
        all_data.to_parquet(gen_data_file)

        return all_data

            



if __name__ == "__main__":
    # get a ;ist opf all the BMUs
    bmu_list = helpers.get_list_of_bmu_ids_from_custom_windfarm_csv()
    for bmu_id in bmu_list:
        try:
            bmu_obj = BMU(bmu_id)
            bmu_obj.update_gen_data = True
            bmu_obj.get_all_gen_data()
        except Exception as e:
            print(f"Error processing {bmu_id}: {e}")