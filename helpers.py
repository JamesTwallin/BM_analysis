import json
import requests
import pandas as pd
import configparser
from bs4 import BeautifulSoup
import os


class APIError(Exception):
    pass

class NoDataError(Exception):
    pass

def get_credentials():
    config = configparser.ConfigParser()
    config.read('config.ini')
    api_key =  config['bmrs']['api_key']
    return api_key

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
    

def load_windfarms_geojson():
    # read in the .geojson file
    root = os.path.dirname(os.path.abspath(__file__))
    filename = root + '/windfarms.geojson'
    # use json
    with open(filename) as f:
        data = json.load(f)
    rows = []
    for feature in data['features']:
        bmu_dict = {}
        for key in feature['properties'].keys():
            bmu_dict[key] = feature['properties'][key]
        try:
            for key in feature['geometry'].keys():
                
                    bmu_dict['lon'] = feature['geometry']['coordinates'][0]
                    bmu_dict['lat'] = feature['geometry']['coordinates'][1]
        except:
            pass
        rows.append(bmu_dict)
    data = pd.DataFrame(rows)
    return data

if __name__ == '__main__':
    load_windfarms_geojson()