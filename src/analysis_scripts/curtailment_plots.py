import ast
import os
from src.utils import helpers
import datetime as dt
import textwrap
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

global project_root_path
project_root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(project_root_path)

def enforce_list(_string_list):
	try:
		assert isinstance(_string_list, list)
		return _string_list
	except:
		string_list = ast.literal_eval(_string_list)
		return string_list

def plot_largest_farms():
    pcey_df = pd.read_csv(os.path.join(project_root_path, 'pcey.csv'))
    _gen_type_df = pcey_df.groupby(['name', 'gen_type']).last().reset_index()
    # grouby by name and sum
    pcey_df = pcey_df.groupby(['name']).sum().reset_index()
    pcey_df = pcey_df.sort_values(by='p50_ideal_yield', ascending=True)
    pcey_df['gen_type'] = pcey_df['name'].map(_gen_type_df.set_index('name')['gen_type'])


    fig = plt.figure(figsize=(8, 30))
    ax = fig.add_subplot(111)
    for gen_type in pcey_df['gen_type'].unique():
        _gen_filt = pcey_df['gen_type'] == gen_type
        _gen_df = pcey_df.copy()
        _gen_df.loc[_gen_filt, 'p50_ideal_yield'] = np.nan
        ax.barh(_gen_df['name'], _gen_df['p50_ideal_yield'], label=gen_type)
    ax.set_xlabel('GWh')


    # save fig
    plot_folder = os.path.join(project_root_path, 'plots')

    if not os.path.exists(plot_folder):
        os.mkdir(plot_folder)
		
    for spine in ['top', 'right','bottom']:
        ax.spines[spine].set_visible(False)
		
    # grid
    ax.grid(axis='x', linestyle='--', alpha=0.5)

    filename = 'largest_farms'
    plot_path = os.path.join(plot_folder, filename)
    plt.tight_layout()
    fig.savefig(f'{plot_path}.png')

	
                

# List of BMUs

if __name__ == "__main__":
	plot_largest_farms()
	


    
