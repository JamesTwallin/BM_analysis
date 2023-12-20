import concurrent.futures
import glob
import matplotlib.pyplot as plt
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

class BMRS:
    def __init__(self):
        self.folder_path = os.path.join(project_root_path, 'data', 'bm_data')
        self.preprocessed_folder_path = os.path.join(project_root_path, 'data', 'preprocessed_data')
        self.bav_data = None
        self.oav_data = None
        self._init_metadata_dict()
        self._load_metadata_dict()

    def _init_metadata_dict(self):
        # glob
        metadata_file = os.path.join(self.folder_path, 'metadata.json')
        files = glob.glob(os.path.join(self.folder_path, '*.parquet'))
        dates = [os.path.basename(file).split('_')[0] for file in files]
        dates = list(set(dates))
        dates.sort()
        metadata_dict = {}
        metadata_dict['processed'] = {}
        metadata_dict['attempted'] = {}
        for date in dates:
            metadata_dict['processed'][date] = True
            metadata_dict['attempted'][date] = True

        with open(metadata_file, 'w') as f:
            json.dump(metadata_dict, f)
        self.metadata_dict = metadata_dict

    
    def _load_metadata_dict(self):
        metadata_file = os.path.join(self.folder_path, 'metadata.json')
        if os.path.exists(metadata_file):
            with open(metadata_file, 'r') as f:
                metadata_dict = json.load(f)
            self.metadata_dict = metadata_dict
        else:
            self._init_metadata_dict()
            self.update_bm_data = True

    def _update_metadata_dict(self):
        metadata_file = os.path.join(self.folder_path, 'metadata.json')
        with open(metadata_file, 'w') as f:
            json.dump(self.metadata_dict, f)

    
    def _update_bm_data(self):
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
                self.session = requests.Session()
                # do chunks of 250 dates at a time
                chunks = [new_dates[i:i + 250] for i in range(0, len(new_dates), 250)]
                for chunk in chunks:
                # Use ThreadPoolExecutor to call API concurrently
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        futures = {executor.submit(self._download_accepted_volumes, date.strftime('%Y-%m-%d')): date for date in chunk}
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
                print("BMRS data is up to date")
        except Exception as e:
            print(f"Error processing BMRS data: {e}")

    def _download_accepted_volumes(self, date_str):
        """
        Downloads and processes BAV and OAV data for a specific date.

        Args:
        date (datetime): The date for which to download the data.
        redo (bool): If True, re-download the data even if it already exists.
        verbose (bool): If True, print detailed log messages.

        Returns:
        True or False
        """
        folder_path = os.path.join(project_root_path, 'data', 'bm_data')

        os.makedirs(folder_path, exist_ok=True)

        oav_filename = os.path.join(folder_path, f'{date_str}_OAV.parquet')
        bav_filename = os.path.join(folder_path, f'{date_str}_BAV.parquet')

        try:
            endpoint = f"https://api.bmreports.com/BMRS/DERBMDATA/v1?APIKey={api_key}&SettlementDate={date_str}&SettlementPeriod=*&BMUnitId=*&BMUnitType=*&LeadPartyName=*&NGCBMUnitName=*&ServiceType=csv"
            response = requests.get(endpoint)

            df = pd.DataFrame(response.text.splitlines())
            df = df[0].str.split(',', expand=True)
            df.drop(0, inplace=True)

            bav_df = df[df[0] == 'BAV'].copy()
            if len(bav_df) > 0:
                bav_df.rename(columns={0: 'HDR', 1: 'BMU_id', 2: 'Settlement Period', 18: 'Total'}, inplace=True)
                bav_df['date'] = pd.to_datetime(date_str)
                bav_df.drop(columns=list(range(3, 18)), inplace=True)
                bav_df.to_parquet(bav_filename, index=False)
            
            oav_df = df[df[0] == 'OAV'].copy()
            if len(oav_df) > 0:
                oav_df.rename(columns={0: 'HDR', 1: 'BMU_id', 2: 'Settlement Period', 18: 'Total'}, inplace=True)
                oav_df['date'] = pd.to_datetime(date_str)
                oav_df.drop(columns=list(range(3, 18)), inplace=True)
                oav_df.to_parquet(oav_filename, index=False)
            return True
        except Exception as e:
            print(f"Error processing BMRS data for {date_str}: {e}")
            return False
        
    def _get_new_processed_dates(self, last_date):
        return [
            date for date in self.metadata_dict['processed']
            if self.metadata_dict['processed'][date] and pd.to_datetime(date) > last_date
        ]
    
    def _read_and_concatenate_dataframes(self, dates, id):
        try:
            file_paths = [os.path.join(self.folder_path, f"{date}_{id}.parquet") for date in dates]
            with concurrent.futures.ThreadPoolExecutor() as executor:
                dataframes = list(executor.map(pd.read_parquet, file_paths))
            return pd.concat(dataframes, ignore_index=True)
        except Exception as e:
            raise Exception(f"_read_and_concatenate_dataframes() failed for {id}: {e}")

    
    def get_all_accepted_volumes_data(self, id, update=False):
        if update:
            self._update_bm_data()

        file_path = os.path.join(self.preprocessed_folder_path, f'{id}_data.parquet')
        if os.path.exists(file_path):
            all_data = pd.read_parquet(file_path)
            last_processed_date = all_data['date'].max()

            new_dates = self._get_new_processed_dates(last_processed_date)
            if new_dates:
                new_data = self._read_and_concatenate_dataframes(new_dates, id)
                all_data = pd.concat([all_data, new_data], ignore_index=True)
                all_data.to_parquet(file_path)
            if id.lower() == 'bav':
                self.bav_data = all_data
            elif id.lower() == 'oav':
                self.oav_data = all_data
            return all_data

        metadata_file = os.path.join(self.folder_path, 'metadata.json')
        with open(metadata_file, 'r') as file:
            metadata_dict = json.load(file)

        processed_dates = [date for date in metadata_dict['processed'] if metadata_dict['processed'][date]]
        all_data = self._read_and_concatenate_dataframes(processed_dates, id)
        all_data['date'] = pd.to_datetime(all_data['date'])
        
        all_data.to_parquet(file_path)
        if id.lower() == 'bav':
            self.bav_data = all_data
        elif id.lower() == 'oav':
            self.oav_data = all_data
        return all_data
    
        

    def get_bav_data_for_bmu(self, bmu_id):
        """
        Processes and filters curtailment data for a specific BMU ID and saves it as a parquet file.
        Loads the data from a file if it already exists.

        Args:
        curtailment_df (DataFrame): The DataFrame containing curtailment data.
        bmu_id (str): The BMU ID to filter the data.

        Returns:
        DataFrame: A DataFrame containing the filtered and processed curtailment data.
        """
        try:
            folder_path = os.path.join(project_root_path, 'data', 'preprocessed_data', bmu_id)
            os.makedirs(folder_path, exist_ok=True)
            filename = os.path.join(folder_path, f'{bmu_id}_curtailment_data.parquet')

            if os.path.exists(filename):
                print('Loading from file')
                return pd.read_parquet(filename)
            
            if self.bav_data is None:
                self.get_all_accepted_volumes_data('BAV')
                

            filt = self.bav_data['BMU_id'].str.contains(bmu_id)
            df = self.bav_data[filt].copy()
            df['Settlement Period'] = df['Settlement Period'].astype(int)
            df.index = pd.to_datetime(df.pop('date'))
            df['utc_time'] = df.index + pd.to_timedelta((df['Settlement Period'] - 1) * 30, unit='minute')
            df.set_index('utc_time', inplace=True)
            df = df.resample('30T').last()
            df['Total'] = df['Total'].astype(float)
            df.to_parquet(filename)
            return df
        except Exception as e:
            raise Exception(f"get_bav_data_for_bmu() failed for {bmu_id}: {e}")





class BMU:
    def __init__(self, bmu_id,update=False):
        self.bmu_id = bmu_id
        self.session = None
        self.update = update
        self.raw_folder_path = os.path.join(project_root_path, 'data', 'raw_gen_data', self.bmu_id)
        self.preprocessed_folder_path = os.path.join(project_root_path, 'data', 'preprocessed_data', self.bmu_id)
        self.plot_folder_path = os.path.join(project_root_path, 'plots', 'generation_data')
        os.makedirs(self.raw_folder_path, exist_ok=True)
        os.makedirs(self.preprocessed_folder_path, exist_ok=True)
        self._load_metadata_dict()

    def _init_metadata_dict(self):
        # glob
        metadata_file = os.path.join(self.raw_folder_path, f'{self.bmu_id}_metadata.json')
        files = glob.glob(os.path.join(self.raw_folder_path, '*.parquet'))
        dates = [os.path.basename(file).split('.')[0] for file in files]
        dates = list(set(dates))
        dates.sort()
        metadata_dict = {}
        metadata_dict['processed'] = []
        metadata_dict['attempted'] = []
        for date in dates:
            metadata_dict['processed'].append(date)
            metadata_dict['attempted'].append(date)

        with open(metadata_file, 'w') as f:
            json.dump(metadata_dict, f)
        self.metadata_dict = metadata_dict
        self.update = True


    def _load_metadata_dict(self):
        metadata_file = os.path.join(self.raw_folder_path, f'{self.bmu_id}_metadata.json')
        if os.path.exists(metadata_file):
            with open(metadata_file, 'r') as f:
                metadata_dict = json.load(f)
            for key in metadata_dict.keys():
                values = metadata_dict[key]
                # remove duplicates
                values = list(set(values))
                # sort
                values.sort()
                metadata_dict[key] = values

            self.metadata_dict = metadata_dict
                # unique and sort
        else:
            self._init_metadata_dict()

    def _update_metadata_dict(self):
        metadata_file = os.path.join(self.raw_folder_path, f'{self.bmu_id}_metadata.json')
        with open(metadata_file, 'w') as f:
            json.dump(self.metadata_dict, f)


    def _update_gen_data(self, start_date=None, end_date=None, redo=False):

        try:
            if start_date is None:
                start_date = pd.to_datetime('2017-01-01')
            if end_date is None:
                end_date = pd.to_datetime('today').floor('D') - pd.Timedelta(days=1)

            date_list = pd.date_range(start_date, end_date, freq='1D').to_list()
            
            if not redo:
                try:
                    attempted_dates = self.metadata_dict['attempted']
                    # remove from the attempted dates which are recent (1 week), becuase we want to re-attempt them in case they failed due to availability of data
                    attempted_dates = [date for date in attempted_dates if pd.to_datetime(date) < pd.to_datetime('today').floor('D') - pd.Timedelta(days=50)]
                    # remove dates that have already been attempted from the date_list
                    new_dates = [date for date in date_list if date.strftime('%Y-%m-%d') not in attempted_dates]
                # Filter dates that are not processed or are within the last 14 days
                except Exception as e:
                    print(f"{self.bmu_id}: {e}")
                    new_dates = date_list
            else:
                new_dates = date_list
            if new_dates:
                self.session = requests.Session()
                # do chunks of 250 dates at a time
                chunks = [new_dates[i:i + 250] for i in range(0, len(new_dates), 250)]
                for chunk in chunks:
                # Use ThreadPoolExecutor to call API concurrently
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        futures = {executor.submit(self._call_api, date.strftime('%Y-%m-%d')): date for date in chunk}
                        for future in concurrent.futures.as_completed(futures):
                            date = futures[future]
                            if date.strftime('%Y-%m-%d') not in self.metadata_dict['attempted']:
                                self.metadata_dict['attempted'].append(date.strftime('%Y-%m-%d'))
                            if future.result():
                                if date.strftime('%Y-%m-%d') not in self.metadata_dict['processed']:
                                    self.metadata_dict['processed'].append(date.strftime('%Y-%m-%d'))                                      
                    # every 250 dates, update the metadata file, to ensure that we don't re-run the same dates if the script crashes
                    self._update_metadata_dict()
            else:
                print(f"{self.bmu_id} is up to date")
        except Exception as e:
            # line number
            print(f"_update_gen_data() failed for {self.bmu_id}: {e}, {sys.exc_info()[-1].tb_lineno}")

    def _call_api(self, date_string):
        if self.session is None:
            self.session = requests.Session()
        endpoint = f"https://api.bmreports.com/BMRS/B1610/v2?APIKey={api_key}&SettlementDate={date_string}&Period=*&NGCBMUnitID={self.bmu_id}&ServiceType=csv"
        try:
            print(f"{self.bmu_id}: {date_string}, downloading data")
            response = self.session.get(endpoint)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                raise APIError(f"{self.bmu_id}: {date_string}, API error: {e}")
            df = pd.DataFrame(response.text.splitlines())
            try:
                assert len(df) > 1
            except AssertionError:
                raise NoDataError(f"{self.bmu_id}: {date_string}, no data")
            df = df[0].str.split(',', expand=True)
            df.columns = df.iloc[1]
            df.drop([0, 1], inplace=True)

            df = df.astype({'SP': 'int', 'Quantity (MW)': 'float'})
            df['Settlement Date'] = pd.to_datetime(df['Settlement Date'])
            df[['Settlement Date', 'SP', 'Quantity (MW)']].to_parquet(f"{self.raw_folder_path}/{date_string}.parquet")
            return True
        except Exception as e:
            print(f"{self.bmu_id}: {date_string}, failed to download data: {e}")
            return False

    def _get_new_processed_dates(self, last_processed_dates):
        # compare the processed dates in the metadata file with the last_processed_dates
        return [
            date for date in self.metadata_dict['processed']
            if date not in last_processed_dates
        ]


    def _read_and_concatenate_dataframes(self, dates):
        file_paths = [os.path.join(self.raw_folder_path, f'{date}.parquet') for date in dates]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            dataframes = list(executor.map(self.__preprocess_gen_data, file_paths))
        return pd.concat(dataframes)

    def __preprocess_gen_data(self,file_path):
        df = pd.read_parquet(file_path)
        df['utc_time'] = df['Settlement Date'] + pd.to_timedelta((df['SP'] - 1) * 30, unit='minute')
        df.set_index('utc_time', inplace=True)
        df.drop(columns=['Settlement Date', 'SP'], inplace=True)
        df = df.resample('30T').last()
        # interpolate missing values (don't do more than 2 consecutive missing values), then fill any remaining missing values with 0
        return df

    def get_all_gen_data(self,redo=False, start_date=None, end_date=None):
        try:
            if self.update:
                self._update_gen_data(start_date, end_date, redo)

            gen_data_file = os.path.join(self.preprocessed_folder_path, f'{self.bmu_id}_generation_data.parquet')
            os.makedirs(self.preprocessed_folder_path, exist_ok=True)
            if os.path.exists(gen_data_file):
                all_data = pd.read_parquet(gen_data_file)
                last_processed_dates = all_data.index.strftime('%Y-%m-%d').unique()
                new_dates = self._get_new_processed_dates(last_processed_dates)
                print(f"Processing {len(new_dates)} new dates")
                if new_dates:
                    new_data = self._read_and_concatenate_dataframes(new_dates)
                    all_data = pd.concat([all_data, new_data])
                    all_data.to_parquet(gen_data_file)

                return all_data

            metadata_file = os.path.join(self.raw_folder_path, f'{self.bmu_id}_metadata.json')
            with open(metadata_file, 'r') as file:
                metadata_dict = json.load(file)

            processed_dates = metadata_dict['processed']
            if not processed_dates:
                NoDataError(f"No data for {self.bmu_id}")
                return None
            all_data = self._read_and_concatenate_dataframes(processed_dates)
            all_data.to_parquet(gen_data_file)

            return all_data
        except Exception as e:
            raise Exception(f"get_all_gen_data() failed for {self.bmu_id}: {e}")

    def plot_data_coverage(self):
        os.makedirs(self.plot_folder_path, exist_ok=True)
        index = pd.date_range('2017-01-01', pd.to_datetime('today').floor('D') - pd.Timedelta(days=1), freq='1D')
        coverage_df = pd.DataFrame(index=index)
        coverage_df['processed'] = False
        coverage_df['attempted'] = False

        for date in self.metadata_dict['processed']:
            coverage_df.loc[date, 'processed'] = True
        for date in self.metadata_dict['attempted']:
            coverage_df.loc[date, 'attempted'] = True

        coverage_df['processed'] = coverage_df['processed'].astype(int)
        coverage_df['attempted'] = coverage_df['attempted'].astype(int)
        # filt where processed = 1
        filt = coverage_df['processed'] == 1
        coverage_df.loc[filt, 'attempted'] = 0.



        fig = plt.figure(figsize=(8, 4))
        ax = fig.add_subplot(111)
        ax.set_title(f"{self.bmu_id} Data Coverage")
        ax.set_xlabel("Date")
        ax.set_ylabel("Data Coverage")
        # get rid of the ticks and tick labels
        ax.set_yticklabels([])
        ax.set_yticks([])
        # vlines
        ax.fill_between(coverage_df.index, coverage_df['processed'], color='green', label='Processed', alpha=0.5, linewidth=0)
        ax.fill_between(coverage_df.index, coverage_df['attempted'], color='orange', label='Attempted', alpha=0.5, linewidth=0)
        ax.legend()
        for spine in ax.spines.values():
            spine.set_visible(False)
        fig.savefig(os.path.join(self.plot_folder_path, f"{self.bmu_id}_data_coverage.png"))

        plt.close(fig)
        

            



if __name__ == "__main__":

    bmus = helpers.get_list_of_bmu_ids_from_custom_windfarm_csv()
    for bmu in bmus:
        try:
            bmu_obj = BMU(bmu, update=True)
            bmu_obj.get_all_gen_data()
        except Exception as e:
            print(f"Error processing {bmu}: {e}")
        finally:
            bmu_obj.plot_data_coverage()


#     # bmrs_obj = BMRS()
#     # bm_data = bmrs_obj.get_all_accepted_volumes_data('BAV', update=True)