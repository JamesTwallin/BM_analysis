import ast
import os
from src.utils import helpers
import datetime as dt
import textwrap
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
# dedent
import textwrap

# Sort the DataFrame
import matplotlib.patches as mpatches


global project_root_path
project_root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(project_root_path)

global color_dict
color_dict = {'offshore wind farm': '#17BEBB', 'onshore wind farm': '#53599A', 'floating wind farm': '#0C120C'}



def enforce_list(_string_list):
	try:
		assert isinstance(_string_list, list)
		return _string_list
	except:
		string_list = ast.literal_eval(_string_list)
		return string_list
    
def plot_largest_farms():
    pcey_df = pd.read_csv(os.path.join(project_root_path, 'pcey.csv'))
    data_points_filt = pcey_df['n_data_points'] > 5
    pcey_df = pcey_df[data_points_filt]
    _gen_type_df = pcey_df.groupby(['name', 'gen_type']).last().reset_index()
    gen_type_dict = dict(zip(_gen_type_df['name'], _gen_type_df['gen_type']))
    # grouby by name and sum
    pcey_df = pcey_df.groupby(['name']).sum().reset_index()
    pcey_df = pcey_df.sort_values(by='p50_ideal_yield', ascending=True)
    pcey_df['gen_type'] = pcey_df['name'].map(gen_type_dict)
    # Sort the DataFrame
    pcey_df.sort_values(by='p50_ideal_yield', ascending=True, inplace=True)

    fig = plt.figure(figsize=(8, 30))
    ax = fig.add_subplot(111)
    # Custom legend handles
    legend_handles = []
    for gen_type in pcey_df['gen_type'].unique():
        handle = mpatches.Patch(color=color_dict.get(gen_type, 'grey'), label=gen_type)
        legend_handles.append(handle)

    # Plot the actual data
    for index, row in pcey_df.iterrows():
        color = color_dict.get(row['gen_type'], 'grey')
        ax.barh(row['name'], row['p50_ideal_yield'], color=color)
        ax.annotate(f"{row['p50_ideal_yield']:.1f} GWh", xy=(row['p50_ideal_yield'], row['name']), va='center', ha='left')

    ax.set_xlabel('GWh')

    # Create the legend with custom handles
    ax.legend(handles=legend_handles, ncol=3, loc='upper center', bbox_to_anchor=(0.15, 1.02))
    # save fig
    plot_folder = os.path.join(project_root_path, 'plots')

    if not os.path.exists(plot_folder):
        os.mkdir(plot_folder)

    for spine in ['top', 'right','bottom']:
        ax.spines[spine].set_visible(False)

    # grid
    ax.grid(axis='x', linestyle='--', alpha=0.5)

    # set the y limits
    ax.set_ylim(-0.5, len(pcey_df) - 0.5)
    # give it more room for the suptitle

    filename = 'largest_farms'
    fig.suptitle('Annual Energy Yield', fontsize=16)
    plot_path = os.path.join(plot_folder, filename)
    # give the suptitle a little more room so it doesn't overlap the title
    plt.tight_layout(rect=[0, 0.03, 1, 0.98])
    # fig.subplots_adjust(top=0.95)

    fig.savefig(f'{plot_path}.png')
    plt.close()


def plot_curtailment():
    pcey_df = pd.read_csv(os.path.join(project_root_path, 'pcey.csv'))
    data_points_filt = pcey_df['n_data_points'] > 5
    pcey_df = pcey_df[data_points_filt]

    _gen_type_df = pcey_df.groupby(['name', 'gen_type']).last().reset_index()
    gen_type_dict = dict(zip(_gen_type_df['name'], _gen_type_df['gen_type']))
    # grouby by name and sum
    pcey_df = pcey_df.groupby(['name']).sum().reset_index()
    pcey_df['curtailment_%'] = pcey_df['curtailment'] / pcey_df['p50_ideal_yield'] * 100
    pcey_df = pcey_df.sort_values(by='curtailment_%', ascending=True)
    pcey_df['gen_type'] = pcey_df['name'].map(gen_type_dict)

    # Sort the DataFrame
    pcey_df.sort_values(by='curtailment_%', ascending=True, inplace=True)

    fig = plt.figure(figsize=(8, 30))
    ax = fig.add_subplot(111)

    # Custom legend handles
    legend_handles = []
    for gen_type in pcey_df['gen_type'].unique():
        handle = mpatches.Patch(color=color_dict.get(gen_type, 'grey'), label=gen_type)
        legend_handles.append(handle)

    # Plot the actual data
    for index, row in pcey_df.iterrows():
        color = color_dict.get(row['gen_type'], 'grey')
        ax.barh(row['name'], row['curtailment_%'], color=color)
        ax.annotate(f"{row['curtailment_%']:.0f} %", xy=(row['curtailment_%'], row['name']), va='center', ha='left')

    ax.legend(handles=legend_handles, ncol=3, loc='upper center', bbox_to_anchor=(0.15, 1.02))
    # save fig
    plot_folder = os.path.join(project_root_path, 'plots')

    if not os.path.exists(plot_folder):
        os.mkdir(plot_folder)

    for spine in ['top', 'right','bottom']:
        ax.spines[spine].set_visible(False)

    # grid
    ax.grid(axis='x', linestyle='--', alpha=0.5)

    # set the y limits
    ax.set_ylim(-0.5, len(pcey_df) - 0.5)
    # give it more room for the suptitle

    # save fig
    plot_folder = os.path.join(project_root_path, 'plots')

    if not os.path.exists(plot_folder):
        os.mkdir(plot_folder)

    for spine in ['top', 'right','bottom']:
        ax.spines[spine].set_visible(False)

    filename = 'curtailment'
    fig.suptitle('Annual Curtailment', fontsize=16)
    plot_path = os.path.join(plot_folder, filename)
    plt.tight_layout(rect=[0, 0.03, 1, 0.98])
    fig.savefig(f'{plot_path}.png')
    plt.close()


def make_md_file():
    md_file_path = os.path.join(project_root_path, 'docs', '_pages', f"energy_yields.md")
    
    text = f'''
    ---
    title: Energy Yields
    author: James Twallin
    date: {dt.datetime.now().strftime('%Y-%m-%d')}
    category: windfarm
    layout: post
    ---
    '''
    text = textwrap.dedent(text)

    for file in ['largest_farms.png', 'curtailment.png']:                            
        text += ("""![]({{ site.baseurl }}""" + f"/assets/{file})\n")    
    text += "\n"

    with open(md_file_path, 'w', encoding='utf-8') as md_file:
        md_file.write(text)
    
                

# List of BMUs

if __name__ == "__main__":
    plot_largest_farms()
    plot_curtailment()  
    make_md_file()
    

	


    
