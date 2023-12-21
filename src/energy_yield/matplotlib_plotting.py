import os, sys

import matplotlib.pyplot as plt
import numpy as np

# mdates
import matplotlib.dates as mdates

# Basemap
from mpl_toolkits.basemap import Basemap



# change the matplotlib font to use open sans
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = 'Open Sans'


global project_root_path
project_root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import matplotlib.pyplot as plt
import os
import pandas as pd

def _get_ax(ax, plot_df, pcey_obj, start, end, y_lims=None):
    # Slice the DataFrame

    df_slice = plot_df[start:end]

    # Plot the data on the provided axis
    ax.plot(df_slice.index, df_slice[pcey_obj.COL_PREDICTED_IDEAL_YIELD], label='Predicted', color = '#0C120C',# dashed
            linestyle='--', linewidth=1)

    ax.plot(df_slice.index, df_slice[pcey_obj.COL_NET_YIELD + '_ok'], label='Generation QC PASS', color = '#53599A', linewidth=2)

    ax.plot(df_slice.index, df_slice[pcey_obj.COL_IDEAL_YIELD + '_ok'], label='Generation - Curtailment QC PASS',   color = '#17BEBB', linewidth=2)

    ax.plot(df_slice.index, df_slice[pcey_obj.COL_NET_YIELD + '_fail'], label='Generation QC FAIL', marker='x', color = '#F26430', linewidth=1)

    ax.plot(df_slice.index, df_slice[pcey_obj.COL_IDEAL_YIELD + '_fail'], label='Generation - Curtailment QC FAIL', marker='x',  color ='#C20114', linewidth=1)
    # Set the title, labels, and legend

    # set the x axis to be the date %y %b
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))

    # y axis limits
    if y_lims is not None:
        ax.set_ylim(y_lims)
    


    ax.set_xlabel('Date')
    ax.set_ylabel('GWh')


    for spine in ['top', 'right','bottom']:
        ax.spines[spine].set_visible(False)

    # y grid lines
    ax.grid(axis='y', linestyle='--', alpha=0.5)

    return ax



def plot_generation(pcey_obj):
    try:
        # Make a folder for the plots
        plot_folder = os.path.join(project_root_path, 'docs', 'assets')
        if not os.path.exists(plot_folder):
            os.mkdir(plot_folder)

        filename = f'2_{pcey_obj.bmu}_pcey'
        plot_path = os.path.join(plot_folder, filename)
        plot_df = pcey_obj.month_df.copy()




        fig = plt.figure(figsize=(8, 15))
        ax1 = fig.add_subplot(611)
        ax2 = fig.add_subplot(612)
        ax3 = fig.add_subplot(613)
        ax4 = fig.add_subplot(614)
        ax5 = fig.add_subplot(615)
        ax6 = fig.add_subplot(616)


        # break the data into 6 parts
        n = len(plot_df)
        n1 = int(n * 1 / 6)
        n2 = int(n * 2 / 6)
        n3 = int(n * 3 / 6)
        n4 = int(n * 4 / 6)
        n5 = int(n * 5 / 6)

        y_lims = [0, plot_df[pcey_obj.COL_PREDICTED_IDEAL_YIELD].max() * 1.1]
        
        ax1 = _get_ax(ax1, plot_df, pcey_obj, n5, n, y_lims)
        ax2 = _get_ax(ax2, plot_df, pcey_obj, n4, n5, y_lims)
        ax3 = _get_ax(ax3, plot_df, pcey_obj, n3, n4, y_lims)
        ax4 = _get_ax(ax4, plot_df, pcey_obj, n2, n3, y_lims)
        ax5 = _get_ax(ax5, plot_df, pcey_obj, n1, n2, y_lims)
        ax6 = _get_ax(ax6, plot_df, pcey_obj, 0, n1, y_lims)


        
        ax2.legend()

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
        plot_folder = os.path.join(project_root_path, 'docs', 'assets')
        if not os.path.exists(plot_folder):
            os.mkdir(plot_folder)

        filename = f'3_{pcey_obj.bmu}_scatter'
        plot_path = os.path.join(plot_folder, filename)
        plot_df = pcey_obj.month_df.copy()
        filt = plot_df[pcey_obj.COL_DAILY_IDEAL_YIELD + '_ok'] > 0
        filt2 = plot_df[pcey_obj.COL_DAILY_IDEAL_YIELD + '_fail'] > 0
        plot_df = plot_df[filt | filt2].copy()

        # Create the plot
        fig = plt.figure(figsize=(8, 8))
        ax = fig.add_subplot(111)
        ax.scatter(plot_df[pcey_obj.COL_DAILY_PREDICTED], plot_df[pcey_obj.COL_DAILY_IDEAL_YIELD + '_ok'], color='#53599A', label='QC PASS',marker='^')
        ax.scatter(plot_df[pcey_obj.COL_DAILY_PREDICTED], plot_df[pcey_obj.COL_DAILY_IDEAL_YIELD + '_fail'], color='#F26430', label='QC FAIL', marker='x')
        x_range = np.linspace(0, plot_df[pcey_obj.COL_DAILY_PREDICTED].max() * 1.1, 100)
        ax.plot(x_range, pcey_obj.fit_dict['slope'] * x_range + pcey_obj.fit_dict['intercept'], color='red', linestyle='--')


        ax.set_xlabel('Predicted Average Daily Generation (GWh)')
        ax.set_ylabel('Actual Average Daily Generation (GWh)')
        ax.legend()

        for spine in ['top', 'right']:
            ax.spines[spine].set_visible(False)


        # grid
        ax.grid(axis='both', linestyle='--', alpha=0.5)
        
        title = (
         f"number of months: {pcey_obj.n_data_points}, "
         f"model $r^2$: {pcey_obj.prediction_r2:.2f}")

        ax.set_title(title)
        fig.suptitle(f'{pcey_obj.bmu} - {pcey_obj.name}', fontsize=16)
        # save the plot
        plt.tight_layout()
        fig.savefig(f'{plot_path}.png')
        plt.close()

    except Exception as e:
        # Error handling
        line_number = sys.exc_info()[-1].tb_lineno
        print(f"plot_scatter(), error: {e}, line: {line_number}")



def plot_p50(pcey_obj):
    
    try:
        # Make a folder for the plots
        plot_folder = os.path.join(project_root_path, 'docs', 'assets')
        if not os.path.exists(plot_folder):
            os.mkdir(plot_folder)
        
        filename = f'4_{pcey_obj.bmu}_p50'
        plot_path = os.path.join(plot_folder, filename)
        
        # Creating the plot
        fig = plt.figure(figsize=(8, 5))
        ax = fig.add_subplot(111)

        ax.bar(pcey_obj.energy_yield_df.index - 0.15, pcey_obj.energy_yield_df['energy_without_curtailment'], width=0.3, label='Expected (without curtailment losses)', color='#17BEBB')
        ax.bar(pcey_obj.energy_yield_df.index + 0.15, pcey_obj.energy_yield_df['energy_with_curtailment'], width=0.3, label='Expected (with curtailment losses)', color='#53599A')
        
        ax.set_xticks(pcey_obj.energy_yield_df.index, ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov','Dec'])
        ax.set_xlabel('Month')
        ax.set_ylabel('GWh')
        ax.legend()

        for spine in ['top', 'right','bottom']:
            ax.spines[spine].set_visible(False)
        
        # y grid lines
        ax.grid(axis='y', linestyle='--', alpha=0.5)

        title = (f"without curtailment: {pcey_obj.p50_ideal_yield:.0f} GWh, "
         f"with curtailment: {pcey_obj.p50_energy_yield:.0f} GWh\n"
         f"losses due to curtailment: {pcey_obj.losses_as_percentage:.2f}%")
        ax.set_title(title)
        suptitle = (f"{pcey_obj.name} Annual Energy Yield")
        fig.suptitle(suptitle, fontsize=16)
        fig.tight_layout()


        # Save the plot
        fig.savefig(f'{plot_path}.png')
        plt.close()
        
    except Exception as e:
        # Error handling
        line_number = sys.exc_info()[-1].tb_lineno
        print(f"plot_p50(), error: {e}, line: {line_number}")

def plot_unseen_df(pcey_obj):
    try:
        # Make a folder for the plots
        plot_folder = os.path.join(project_root_path, 'docs', 'assets')
        if not os.path.exists(plot_folder):
            os.mkdir(plot_folder)

        filename = f'1_{pcey_obj.bmu}_unseen'
        plot_path = os.path.join(plot_folder, filename)
        plot_df = pcey_obj.unseen_df.copy()
        # Create the plot
        fig = plt.figure(figsize=(8, 4))
        ax = fig.add_subplot(111)
        # LINE PLOT
        ax.plot(plot_df.index, plot_df[pcey_obj.COL_PREDICTED_IDEAL_YIELD]*2000., label='Predicted', color = '#0C120C', linewidth=1, alpha=0.7, linestyle='--')
        ax.fill_between(plot_df.index, plot_df[pcey_obj.COL_IDEAL_YIELD]*2000., color = '#17BEBB', alpha=0.3, label='Observed (without curtailment losses)', linewidth=0)


        ax.set_xlabel('Date')
        ax.set_ylabel('Power Output (MW)')
        ax.set_ylim([0, plot_df[pcey_obj.COL_PREDICTED_IDEAL_YIELD].max()*2000. * 1.1])
        ax.legend()

        for spine in ['top', 'right']:
            ax.spines[spine].set_visible(False)


        # rotate the x axis labels by 45 degrees
        plt.xticks(rotation=45)


        # grid
        ax.grid(axis='y', linestyle='--', alpha=0.5)
        
        title = ("Data not seen by ML model\n")

        ax.set_title(title)
        fig.suptitle(f'{pcey_obj.bmu} - {pcey_obj.name}', fontsize=16)
        # save the plot
        plt.tight_layout()
        fig.savefig(f'{plot_path}.png')
        plt.close()

    except Exception as e:
        # Error handling
        line_number = sys.exc_info()[-1].tb_lineno
        print(f"plot_unseen_df(), error: {e}, line: {line_number}")


def plot_lat_lons(name,wf_locations, weather_nodes):
    try:
        # Make a folder for the plots
        plot_folder = os.path.join(project_root_path, 'docs', 'assets')
        if not os.path.exists(plot_folder):
            os.mkdir(plot_folder)

        filename = f'{name.lower()}_lat_lons'.replace(' ', '_')
        plot_path = os.path.join(plot_folder, filename)
        # Create the plot
        fig = plt.figure(figsize=(7, 5))
        ax = fig.add_subplot(111)
        # get the average lat and lon
        centre_lat, centre_lon = np.mean(wf_locations, axis=0)
        m = Basemap(projection='lcc', lat_0=centre_lat, lon_0=centre_lon,
                    resolution='i', llcrnrlat=centre_lat - .5, urcrnrlat=centre_lat + .5,
                    llcrnrlon=centre_lon - 1.5, urcrnrlon=centre_lon + 1.5, ax=ax)
        
                    
        m.drawcoastlines()
        m.drawcountries()
        m.fillcontinents(color='lightgrey')
        # needs a scale
        scale_lat = centre_lat - 0.4  # Adjust as needed
        scale_lon = centre_lon - .95 # Adjust as needed
        scale_length = 50             # Length of the scale bar in kilometers

        # Draw the scale bar
        m.drawmapscale(scale_lon, scale_lat, scale_lon, scale_lat, scale_length, barstyle='fancy')



        # Offset values in data coordinates
        era5_text_offset_x = 30000
        wf_text_offset_x = 5000
          # Adjust these values as needed

        for wf_location in wf_locations:
            lat, lon = wf_location
            x, y = m(lon, lat)
            m.plot(x, y, marker= 'x', markersize=10, color='black')
            # annotate the point with offset
            plt.annotate(f"Wind Farm: {lat:.2f}, {lon:.2f}", 
                        xy=(x, y), 
                        xytext=(x + wf_text_offset_x, y), 
                        fontsize=10, 
                        color='black')

        for weather_node in weather_nodes:
            lat, lon = weather_node
            x, y = m(lon, lat)
            m.plot(x, y, 'x', markersize=10, color='blue')
            # annotate the point with offset
            plt.annotate(f"ERA5: {lat:.2f}, {lon:.2f}", 
                        xy=(x, y), 
                        xytext=(x - era5_text_offset_x, y), 
                        fontsize=10, 
                        color='blue')

        


        # add a legend
        fig.suptitle(f'{name}', fontsize=16)
        ax.set_title('Wind Farm Location and the ERA5 node used for modelling', fontsize=10)
        plt.tight_layout()

        # save the plot
        fig.savefig(f'{plot_path}.png')
        plt.close(fig)

    except Exception as e:
        # Error handling
        line_number = sys.exc_info()[-1].tb_lineno
        print(f"plot_lat_lons(), error: {e}, line: {line_number}")

    finally:
        plt.close(fig)
        
