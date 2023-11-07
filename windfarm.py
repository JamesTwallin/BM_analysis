
import pandas as pd 
import os, sys
import requests
import concurrent.futures
import itertools

from helpers import get_credentials, get_windfarm_details, load_windfarms_geojson

global api_key

class NoDataError(Exception):
    pass

class APIError(Exception):
    pass

def download_gen_data(start, end,bmu_id):
    date_range = pd.date_range(start, end,freq='1D')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(_get_gen_df, date_range, itertools.repeat(bmu_id))




def _get_gen_df(date,bmu_id):
    root = os.path.dirname(os.path.abspath(__file__))
    # set the filename
    if not os.path.exists(root + '/raw_gen_data'):
        os.makedirs(root + '/raw_gen_data')
    date_string = date.strftime('%Y-%m-%d')
    filename = root + f'/raw_gen_data/{bmu_id}_{date_string}.parquet'
        # check if the file exists
    if os.path.exists(filename):
        print(f"File exists: {filename}")
        return None
    else:
        try:
            # create the url
            endpoint = f"""https://api.bmreports.com/BMRS/B1610/v2?APIKey={api_key}&SettlementDate={date_string}&Period=*&NGCBMUnitID={bmu_id}&ServiceType=csv"""
            response = requests.get(endpoint)
            if response.status_code != 200:
                raise APIError(f"API call failed for {date_string}")
            # the endpoint returns a csv file, so we can use pandas to read it
            df = pd.DataFrame(response.text.splitlines())

            # split ',' delimited columns into separate columns
            df = df[0].str.split(',', expand=True)
            # drop the first row
            df.drop(0, inplace=True)
            # assert that length is greater than 1
            if len(df) < 1:
                # make blank dataframe
                df = pd.DataFrame(columns=['Settlement Date','SP', 'Quantity (MW)'])
                df.to_parquet(f'{filename}')
                raise NoDataError(f"No data for {bmu_id} on {date_string}")
 
            df.columns = df.iloc[0]
            df.drop(1, inplace=True)
            # reindex the dataframe
            df.reset_index(drop=True, inplace=True)
            df[['Settlement Date','SP', 'Quantity (MW)']].to_parquet(f'{filename}')
            print(f"Saved {filename}")
            return None
        except NoDataError as e:
            print(e)
            return None
        except APIError as e:
            print(e)
            return None
        except Exception as e:
            print(e)
            # tell me the line number of the error
            exc_type, exc_obj, exc_tb = sys.exc_info()

            print(f"Error: {filename} {exc_tb.tb_lineno}")
            return None



        
if __name__ == '__main__':
    api_key = None
    if api_key is None:
        api_key = get_credentials()
    
    

    
    start = '2017-01-01'
    end = '2023-11-05'
    windfarm_details = load_windfarms_geojson()
    # drop where 'capacity' is nan
    windfarm_details.dropna(subset=['capacity'], inplace=True)
    # get bmrs_ids, some are lists, so split them
    bmrs_ids = [item for sublist in windfarm_details['bmrs_id'].tolist() for item in sublist]

    # loop through the BMU ids
    # df = _get_gen_df(date=pd.to_datetime('2023-10-01'), bmu_id='HRSTW-1')
    for bmu_id in bmrs_ids:
        download_gen_data(start=start, end=end, bmu_id=bmu_id)
        # get_wf_df(start=start, end=end, bmu_id=bmu)

