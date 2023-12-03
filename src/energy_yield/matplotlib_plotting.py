import os, sys
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats


# change the matplotlib font to use open sans
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = 'Open Sans'

COLOURS = ['#3F7D20', '#A0DDE6', '#542e71','#3F7CAC','#698F3F']

global project_root_path
project_root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import matplotlib.pyplot as plt
import os
import pandas as pd

def _get_ax(ax, plot_df, pcey_obj, start, end):
    # Slice the DataFrame
    df_slice = plot_df[start:end]

    # Plot the data on the provided axis
    ax.plot(df_slice.index, df_slice[pcey_obj.COL_PREDICTED_IDEAL_YIELD], label='Predicted', marker='x')
    ax.plot(df_slice.index, df_slice[pcey_obj.COL_NET_YIELD + '_ok'], label='Generation', marker='x')
    ax.plot(df_slice.index, df_slice[pcey_obj.COL_IDEAL_YIELD + '_ok'], label='Generation - Curtailment', marker='x')

    # Set the title, labels, and legend


    ax.set_xlabel('Month')
    ax.set_ylabel('GWh')
    ax.legend()

    for spine in ['top', 'right','bottom']:
        ax.spines[spine].set_visible(False)

    # y grid lines
    ax.grid(axis='y', linestyle='--', alpha=0.5)

    return ax



def plot_generation(pcey_obj):
    try:
        # Make a folder for the plots
        plot_folder = os.path.join(project_root_path, 'plots')
        if not os.path.exists(plot_folder):
            os.mkdir(plot_folder)

        filename = f'2_{pcey_obj.bmu}_pcey'
        plot_path = os.path.join(plot_folder, filename)
        plot_df = pcey_obj.month_df.copy()




        fig = plt.figure(figsize=(10, 15))
        ax1 = fig.add_subplot(411)
        ax2 = fig.add_subplot(412)
        ax3 = fig.add_subplot(413)
        ax4 = fig.add_subplot(414)

        # break the data into 4 parts
        n = len(plot_df)
        n1 = int(n / 4)
        n2 = int(n / 2)
        n3 = int(n * 3 / 4)

        # Plot the data
        ax1 = _get_ax(ax1, plot_df, pcey_obj, 0, n1)
        ax2 = _get_ax(ax2, plot_df, pcey_obj, n1, n2)
        ax3 = _get_ax(ax3, plot_df, pcey_obj, n2, n3)
        ax4 = _get_ax(ax4, plot_df, pcey_obj, n3, n)

        fig.suptitle(f'{pcey_obj.bmu} - {pcey_obj.name}', fontsize=16)


        plt.tight_layout()


        # Save the plot
        plt.savefig(f'{plot_path}.png')
        plt.close()

    except Exception as e:
        # Error handling
        line_number = sys.exc_info()[-1].tb_lineno
        print(f"plot_generation(), error: {e}, line: {line_number}")


def plot_scatter(pcey_obj):
    try:
        # Make a folder for the plots
        plot_folder = os.path.join(project_root_path, 'plots')
        if not os.path.exists(plot_folder):
            os.mkdir(plot_folder)

        filename = f'1_{pcey_obj.bmu}_scatter'
        plot_path = os.path.join(plot_folder, filename)
        plot_df = pcey_obj.month_df.copy()
        filt = plot_df[pcey_obj.COL_DAILY_IDEAL_YIELD + '_ok'] > 0
        filt2 = plot_df[pcey_obj.COL_DAILY_IDEAL_YIELD + '_fail'] > 0
        plot_df = plot_df[filt | filt2].copy()

        # Create the plot
        fig = plt.figure(figsize=(10, 10))
        ax = fig.add_subplot(111)
        ax.scatter(plot_df[pcey_obj.COL_DAILY_PREDICTED], plot_df[pcey_obj.COL_DAILY_IDEAL_YIELD + '_ok'], color='darkblue', label='QC PASS')
        ax.scatter(plot_df[pcey_obj.COL_DAILY_PREDICTED], plot_df[pcey_obj.COL_DAILY_IDEAL_YIELD + '_fail'], color='orange', label='QC FAIL')

        x_range = np.linspace(0, plot_df[pcey_obj.COL_DAILY_PREDICTED].max() * 1.1, 100)
        ax.plot(x_range, pcey_obj.fit_dict['slope'] * x_range + pcey_obj.fit_dict['intercept'], color='red', linestyle='--')


        ax.set_xlabel('Predicted Average Daily Generation (GWh)')
        ax.set_ylabel('Actual Average Daily Generation (GWh)')
        ax.legend()

        for spine in ['top', 'right']:
            ax.spines[spine].set_visible(False)

        fig.suptitle(f'{pcey_obj.bmu} - {pcey_obj.name}', fontsize=16)
        # save the plot
        fig.savefig(f'{plot_path}.png')
        plt.close()

    except Exception as e:
        # Error handling
        line_number = sys.exc_info()[-1].tb_lineno
        print(f"plot_scatter(), error: {e}, line: {line_number}")



def plot_p50(pcey_obj):
    
    try:
        # Make a folder for the plots
        plot_folder = os.path.join(project_root_path, 'plots')
        if not os.path.exists(plot_folder):
            os.mkdir(plot_folder)
        
        filename = f'3_{pcey_obj.bmu}_p50'
        plot_path = os.path.join(plot_folder, filename)
        
        # Creating the plot
        fig = plt.figure(figsize=(10, 6))
        ax = fig.add_subplot(111)

        ax.bar(pcey_obj.energy_yield_df.index - 0.15, pcey_obj.energy_yield_df['energy_without_curtailment'], width=0.3, label='Expected (without curtailment losses)')
        ax.bar(pcey_obj.energy_yield_df.index + 0.15, pcey_obj.energy_yield_df['energy_with_curtailment'], width=0.3, label='Expected (with curtailment losses)')
        
        ax.set_xticks(pcey_obj.energy_yield_df.index, ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov','Dec'])
        ax.set_xlabel('Month')
        ax.set_ylabel('GWh')
        ax.legend()

        for spine in ['top', 'right','bottom']:
            ax.spines[spine].set_visible(False)
        
        # y grid lines
        ax.grid(axis='y', linestyle='--', alpha=0.5)

        title = (f"{pcey_obj.name} Annual Energy Yield\n"
         f"without curtailment: {pcey_obj.p50_ideal_yield:.0f} GWh, "
         f"with curtailment: {pcey_obj.p50_energy_yield:.0f} GWh\n"
         f"BMU: {pcey_obj.bmu}, number of months: {pcey_obj.n_data_points}, "
         f"model r-squared: {pcey_obj.prediction_r2:.2f}, "
         f"losses due to curtailment: {pcey_obj.losses_as_percentage:.2f}%")

        fig.suptitle(title)
        fig.tight_layout()


        # Save the plot
        fig.savefig(f'{plot_path}.png')
        plt.close()
        
    except Exception as e:
        # Error handling
        line_number = sys.exc_info()[-1].tb_lineno
        print(f"plot_p50(), error: {e}, line: {line_number}")
