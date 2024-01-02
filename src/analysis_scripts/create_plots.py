import ast
import os
from src.utils import helpers
import datetime as dt
import textwrap
import pandas as pd
from src.energy_yield import matplotlib_plotting

global project_root_path
project_root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(project_root_path)

description_dict = {'scatter': """### Scatter of Average Daily Generation \n""", 
                    'unseen': """### Unseen Data \nscatter plot of wind speed vs power output for unseen data""",
                    'pcey': """### Hindcast \nplot of predicted vs actual energy yield""",
                    'p50': """### Expected Annual Yield \nThis is the typical output for the windfarm, using the hindcast data and the actual data to create a profile for the typical year. This is the P50 energy yield, which is the expected annual energy yield with a 50% probability of being exceeded"""}

def enforce_list(_string_list):
    try:
        assert isinstance(_string_list, list)
        return _string_list
    except Exception as e:
        print(e)
        string_list = ast.literal_eval(_string_list)
        return string_list
     
def get_lat_lons(pcey_rows):
    lats = pcey_rows['lat'].tolist()
    lons = pcey_rows['lon'].tolist()
    # if the lon is > 180, then it is in the western hemisphere and needs to be made negative
    lons = [lon-360 if lon > 180 else lon for lon in lons]
    # zip the lat and lon together
    return list(zip(lats, lons))

def get_era5_lat_lons(pcey_rows):
    lats = pcey_rows['era5_lat'].tolist()
    lons = pcey_rows['era5_lon'].tolist()
        # if the lon is > 180, then it is in the western hemisphere and needs to be made negative
    lons = [lon-360 if lon > 180 else lon for lon in lons]
    # zip the lat and lon together
    return list(zip(lats, lons))



def append_html_to_md(windfarm_df):
    # with open(md_file_path, 'a') as md_file:

    # your code to write data

        for index, row in windfarm_df.iterrows():
            date_string = dt.datetime.now().strftime("%Y-%m-%d")
            name = row['name']
            md_file_path = os.path.join(project_root_path, 'docs', '_posts', f"{date_string}-{name.lower().replace(' ', '_')}.md")
            
            with open(md_file_path, 'w', encoding='utf-8') as md_file:
                try:
                    name = row['name']
                    # Add BMU name as a header with a proper utf-8 encoding
                    # get the name from the PCYE csv
                    filt = pcey_df['name'] == name
                    pcey_rows = pcey_df[filt]
                    energy_yield = pcey_rows['p50_energy_yield'].sum()
                    wf_tuple = get_lat_lons(pcey_rows)
                    era5_tuple = get_era5_lat_lons(pcey_rows)
                    matplotlib_plotting.plot_lat_lons(name, wf_tuple, era5_tuple)
                    # print(f"BMU {name} has {len(lats)} lat/lon pairs")


                    # add this:

                    text = f'''
                    ---
                    title: {name}
                    author: James Twallin
                    date: {date_string}
                    category: windfarm
                    layout: post
                    ---
                    '''

                    text = textwrap.dedent(text)


                    filename = f'{name.lower()}_lat_lons'.replace(' ', '_')
                    text += ("""![]({{ site.baseurl }}""" + f"/assets/{filename}.png)\n\n")    

    
                    # remove the first line
                    text = text.split('\n', 1)[1]
                    text += f"""{name} P50 Energy Yield: {energy_yield:.2f} GWh\n\n"""
                    bmus = enforce_list(row['bmrs_id'])
                    assert len(bmus) > 0
                    bmus.sort()

                    for bmu in bmus:
                        # Ingredients
                        text += f"{bmu}\n-------------\n"
                        # list dir with a wildcard
                        file_list = os.listdir(os.path.join(project_root_path, 'docs', 'assets',))
                        file_list = [file for file in file_list if bmu in file and 'png' in file]
                        assert len(file_list) > 0
 
                        for file in file_list:      
                            # get the text associated with the key
                            description_text = description_dict[file.split('_')[2].split('.')[0]]
                            # remove indentation
                            description_text = textwrap.dedent(description_text)
                            text += f"{description_text}\n"        
                            text += ("""![]({{ site.baseurl }}""" + f"/assets/{file})\n")    
                        text += "\n"


                            
                    md_file.write(text)
                # got to the plots folder and get the html files with contain any of the BMU names
                
                # bmu_contents = bmu_file.read()
                # # Append the HTML content
                # md_file.write(f"{bmu_contents}\n")
                except AssertionError:
                    print(f"BMU {name} has no plots")
                    # close the file
                    md_file.close()
                    if os.path.exists(md_file_path):
                        os.remove(md_file_path)
                # filenotfounderror
                except FileNotFoundError:
                    pass


                except Exception as e:
                    print(f"An error occurred while processing BMU {name}: {e}")
                    print(f"BMU {name} has no plots")
                    # close the file
                    md_file.close()
                    os.remove(md_file_path)

                

# List of BMUs

if __name__ == "__main__":
    windfarm_df = helpers.read_custom_windfarm_csv()

    # read the pcey.csv
    global pcey_df
    pcey_df = pd.read_csv(os.path.join(project_root_path, 'pcey.csv'))

    # Path to your markdown file
    append_html_to_md(windfarm_df)
