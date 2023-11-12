import glob
import json
import requests
import numpy as np
import pandas as pd
import configparser
from bs4 import BeautifulSoup
import os
import time
import concurrent.futures
# from tqdm import tqdm

# Global variable for project root path
project_root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def get_credentials():
    config = configparser.ConfigParser()
    config.read('config.ini')
    api_key =  config['bmrs']['api_key']
    return api_key





class APIError(Exception):
    pass

class NoDataError(Exception):
    pass




def get_windfarm_details():
    root = os.path.dirname(os.path.abspath(__file__))
    filename = root + '/windfarm_details.parquet'
    if os.path.exists(filename):
        windfarm_details_ = pd.read_parquet(filename)

    else:
    
        
        url = 'https://query.wikidata.org/sparql'
        query = """SELECT DISTINCT ?item ?itemLabel ?bmrs_id ?capacity ?typeLabel ?lon ?lat WHERE {
        ?item wdt:P11610 ?bmrs_id.
        OPTIONAL { ?item wdt:P2109 ?capacity. }
        OPTIONAL {
            ?item wdt:P31 ?type.
            ?type wdt:P279+ wd:Q159719.
        }
        OPTIONAL { 
            ?item p:P625 ?coordinate.
            ?coordinate ps:P625 ?coord.
            ?coordinate psv:P625 ?coordinate_node.
            ?coordinate_node wikibase:geoLongitude ?lon.
            ?coordinate_node wikibase:geoLatitude ?lat. 
        }
        SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }
        }"""
        r = requests.get(url, params = {'format': 'json', 'query': query})
        data = r.json()
        row_list = []
        for wf in data['results']['bindings']:
            wf_dict  = {}
            for key in wf.keys():
                wf_dict[key] = wf[key]['value']
            row_list.append(wf_dict)
        windfarm_details_ = pd.DataFrame(row_list)
        windfarm_details_
        filt = windfarm_details_['lat'].isnull()
        windfarm_details_ = windfarm_details_.loc[~filt]
        for col in windfarm_details_.columns:
            try:
                windfarm_details_[col] = windfarm_details_[col].astype('float')
            except:
                pass
        windfarm_details_['name'] = windfarm_details_.apply(lambda x: _get_windfarm_name(x['itemLabel']), axis=1)
        windfarm_details_['type'] = windfarm_details_.apply(lambda x: _get_windfarm_name(x['typeLabel']), axis=1)
        filt = windfarm_details_['type'] == 'wind farm'
        windfarm_details_.loc[filt, 'type'] = 'onshore wind farm'

        windfarm_details_.to_parquet(filename)

        # where type cotains 'wind farm'
        # convert 'wind farm' to 'pnshore wind farm'

        filt = windfarm_details_['type'].str.contains('wind farm')

        windfarm_details_ = windfarm_details_.loc[filt]

        # where bmrs_id = 'BTUIW-2'
        filt = windfarm_details_['bmrs_id'] == 'BTUIW-2'
        windfarm_details_.loc[filt, 'capacity'] = 68.0
        return windfarm_details_
    
def get_uk_windfarms():
    url = 'https://query.wikidata.org/sparql'
    # Q194356 is wind farm
    # P17 is country
    query = """
    SELECT ?windFarm ?windFarmLabel ?country ?countryLabel
    WHERE {
    ?windFarm wdt:P31 wd:Q194356.  # Instances of wind farm
    ?windFarm wdt:P17 ?country.    # Country property
    SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }"""
    r = requests.get(url, params = {'format': 'json', 'query': query})
    data = r.json()
    row_list = []
    for wf in data['results']['bindings']:
        wf_dict  = {}
        for key in wf.keys():
            wf_dict[key] = wf[key]['value']
        row_list.append(wf_dict)
    windfarm_details = pd.DataFrame(row_list)
    return windfarm_details


        
def _get_windfarm_name(id):
    try:
        # Make a GET request to the web page
        url = "https://www.wikidata.org/wiki/" + id
        response = requests.get(url)

        # Parse the HTML content with BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        # Find the title element and extract the text
        title_element = soup.find('title')
        title = title_element.text
        # Remove the Wikidata prefix and suffix
        title = title.replace(' - Wikidata', '')
        if title == 'wind farm':
            title = 'onshore wind farm'
        return title
    except:
        return ''
        
def get_windfarm_type(url):
    try:
        item_id = url.split('/')[-1]
        endpoint_url = 'https://query.wikidata.org/sparql'
        
        query = f"""
        SELECT ?property ?propertyLabel ?value ?valueLabel
        WHERE {{
        wd:{item_id} wdt:P31 ?value .
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
        }}
        """
        success = False
        while not success:
            r = requests.get(endpoint_url, params={'format': 'json', 'query': query})
            if r.status_code == 200:
                success = True
                data = r.json()
                for wf in data['results']['bindings']:
                    return wf['valueLabel']['value']
            else:
                print(f"Retry due to HTTP status: {r.status_code}, {item_id}")
                time.sleep(10)
    except Exception as e:
        print(f"Error: {e}")
        return ''

def load_windfarms_geojson(get_type=True):
    # read in the .geojson file
    data = _load_raw_windfarms_geojson()
    rows = []
    for feature in data['features']:

        bmu_dict = {}
        if ('wiki' in feature['id']) & get_type:
            bmu_dict['gen_type'] = get_windfarm_type(feature['id'])
        for key in feature['properties'].keys():
            bmu_dict[key] = feature['properties'][key]
        try:
            for key in feature['geometry'].keys():
                
                    bmu_dict['lon'] = feature['geometry']['coordinates'][0]
                    bmu_dict['lat'] = feature['geometry']['coordinates'][1]
        except:
            pass
        # if the length of the bmrs_id is 1 then it is a string
        rows.append(bmu_dict)
    data = pd.DataFrame(rows)
    return data

def _load_raw_windfarms_geojson():
    root = os.path.dirname(os.path.abspath(__file__))
    filename = root + '/windfarms.geojson'
    # use json
    with open(filename) as f:
        data = json.load(f)
    return data

def read_custom_windfarm_csv():
    data = pd.read_csv("windfarms_details_jt.csv")
    return data


def move_file(file):
    # split the file on '\'
    folder_name = file.split('\\')[0]
    file_name = file.split('\\')[1]
    # print(folder_name)
    # print(file_name)
    # split the file_name on '_'
    bmu_id = file_name.split('_')[0]
    date = file_name.split('_')[1].split('.')[0]
    # print(f"bmu_id: {bmu_id}, date: {date}")
    new_file_name = folder_name + '/' + bmu_id + '/' + date + '.parquet'
    # print(new_file_name)
    # check if the folder exists
    if not os.path.exists(folder_name + '/' + bmu_id):
        os.makedirs(folder_name + '/' + bmu_id)
    # move the file
    os.rename(file, new_file_name)


def get_nearest_weather_data(weather_data,lat,lon):
    _df = weather_data.loc[(weather_data['lat'] == lat) & (weather_data['lon'] == lon)]
    # drop lat and lon columns
    _df = _df.drop(columns=['lat', 'lon'])
    # rename the index to utc_time
    _df.index.name = 'utc_time'
    _df = _df.resample('1H').last()
    return _df

# a function which takes a lat, lon and returns the nearest values from a list of lat, lon values
def get_nearest_lat_lon(lat, lon):
    data_file = os.path.join(project_root_path,'data', 'weather_data', '2023_01_northward_wind_at_100_metres.parquet')
    df = pd.read_parquet(data_file)

    lat_lon_list = df[['lat', 'lon']].drop_duplicates().to_numpy()

    lat_rad, lon_rad = np.radians(lat), np.radians(lon)
    lat_lon_list_rad = np.radians(lat_lon_list)

    dlon = lon_rad - lat_lon_list_rad[:, 1]
    dlat = lat_rad - lat_lon_list_rad[:, 0]
    a = np.sin(dlat / 2) ** 2 + np.cos(lat_rad) * np.cos(lat_lon_list_rad[:, 0]) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    dist = 6371 * c

    idx = np.argmin(dist)

    return lat_lon_list[idx]


