import concurrent.futures
import requests
import pandas as pd
import os


from helpers import get_credentials

global api_key

def get_all_accepted_volumes(start_date, end_date):
    date_range = pd.date_range(start_date, end_date,freq='1D')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(_download_accepted_volumes, date_range)


def _download_accepted_volumes(date):
    date = date.strftime('%Y-%m-%d')
    root = os.path.dirname(os.path.abspath(__file__))
    # make the folder if it doesn't exist
    if not os.path.exists(root + '/bm_data'):
        os.makedirs(root + '/bm_data')
    oav_filename = root + f'/bm_data/{date}_OAV.parquet'
    bav_filename = root + f'/bm_data/{date}_BAV.parquet'
    # check if the file exists
    if os.path.exists(oav_filename) and os.path.exists(bav_filename):
        print(f"Files exist: {oav_filename}, {bav_filename}")
    if os.path.exists(oav_filename) and os.path.exists(bav_filename):
        return None
    else:
        # create the url
        print(f"Downloading data for {date}")
        endpoint = f"""https://api.bmreports.com/BMRS/DERBMDATA/v1?APIKey={api_key}&SettlementDate={date}&SettlementPeriod=*&BMUnitId=*&BMUnitType=*&LeadPartyName=*&NGCBMUnitName=*&ServiceType=csv"""
        response = requests.get(endpoint)



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
        
if __name__ == "__main__":
    api_key = None
    if api_key is None:
        api_key = get_credentials()
    start_date = '2017-01-01'
    end_date = '2023-11-01'
    get_all_accepted_volumes(start_date, end_date)