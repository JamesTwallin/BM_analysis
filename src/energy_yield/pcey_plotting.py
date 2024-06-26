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



@staticmethod
def plot_generation(pcey_obj):
    try:
        # make a folder for the plots
        plot_folder = os.path.join(project_root_path, 'plots')
        if not os.path.exists(plot_folder):
            os.mkdir(plot_folder)

        filename = f'2_{pcey_obj.bmu}_pcey'
        plot_path = os.path.join(plot_folder, filename)
        plot_df = pcey_obj.month_df.copy()


        fig = go.Figure()
        fig.add_trace(go.Scattergl(x=plot_df.index, y=plot_df[pcey_obj.COL_PREDICTED_IDEAL_YIELD], name='Predicted',mode='lines+markers'))
        fig.add_trace(go.Scattergl(x=plot_df.index, y=plot_df[pcey_obj.COL_NET_YIELD + '_ok'], name= 'Generation', mode='lines+markers'))
        fig.add_trace(go.Scattergl(x=plot_df.index, y=plot_df[pcey_obj.COL_IDEAL_YIELD + '_ok'], name='Generation - Curtailment', mode='lines+markers'))
        title = f"<span style='font-size: 20px; font-weight: bold;'>{pcey_obj.name}</span><br><span style='font-size: 16px;'>{pcey_obj.bmu}</span>"
        fig.update_layout(title=title, xaxis_title='Month', yaxis_title='GWh')
        # whit theme
        fig.update_layout(template='plotly_white')
        # save to html
        fig.write_html(f'{plot_path}.html', full_html=False,config={'displayModeBar': False, 'displaylogo': False})

        plt.close()
    except Exception as e:
        # line number
        line_number = sys.exc_info()[-1].tb_lineno
        print(f"plot_generation(), error: {e}, line: {line_number}")

@staticmethod
def plot_scatter(pcey_obj):
    try:
        plot_df = pcey_obj.month_df.copy()
        filt = plot_df[pcey_obj.COL_DAILY_IDEAL_YIELD + '_ok'] > 0
        filt2 = plot_df[pcey_obj.COL_DAILY_IDEAL_YIELD + '_fail'] > 0
        plot_df = plot_df[filt | filt2].copy()

        # make a folder for the plots
        plot_folder = os.path.join(project_root_path, 'plots')
        if not os.path.exists(plot_folder):
            os.mkdir(plot_folder)
        filename = f'1_{pcey_obj.bmu}_scatter'
        plot_path = os.path.join(plot_folder, filename)


        # three column, 1 row subplots with Go

        colours = COLOURS
        
        #subplot 1
        fig = go.Figure()

        #subplot 3
        text = [f"QC PASS <br>Predicted: {pred:.2f} GWh<br>Actual: {actual:.2f} GWh<br>Month: {month}" for pred, actual, month in zip(plot_df[pcey_obj.COL_DAILY_PREDICTED], plot_df[pcey_obj.COL_DAILY_IDEAL_YIELD + '_ok'], plot_df.index.strftime('%b %Y'))]
        fig.add_trace(go.Scatter(x=plot_df[pcey_obj.COL_DAILY_PREDICTED], y=plot_df[pcey_obj.COL_DAILY_IDEAL_YIELD + '_ok'], mode='markers',marker_color = 'darkblue',
                                    hovertext=text,hoverinfo='text', showlegend=False))
        text = [f"QC FAIL <br>Predicted: {pred:.2f} GWh<br>Actual: {actual:.2f} GWh<br>Month: {month}" for pred, actual, month in zip(plot_df[pcey_obj.COL_DAILY_PREDICTED], plot_df[pcey_obj.COL_DAILY_IDEAL_YIELD + '_fail'], plot_df.index.strftime('%b %Y'))]
        fig.add_trace(go.Scatter(x=plot_df[pcey_obj.COL_DAILY_PREDICTED], y=plot_df[pcey_obj.COL_DAILY_IDEAL_YIELD + '_fail'], mode='markers',marker_color = 'orange',
                                    hovertext=text,hoverinfo='text', showlegend=False))
        x_range = np.arange(0, plot_df[pcey_obj.COL_DAILY_PREDICTED].max()*1.5, 0.1)


        fig.add_trace(go.Scatter(x=x_range, y=pcey_obj.fit_dict['slope'] * x_range + pcey_obj.fit_dict['intercept'], mode='lines',
                                  line=dict(color='red', dash='dash'), showlegend=False))
        fig.update_xaxes(title_text='Predicted Daily Mean Generation (GWh)',titlefont=dict(size=12))
        fig.update_yaxes(title_text='Actual Daily Mean Generation (GWh)',titlefont=dict(size=12))
        fig.update_yaxes(range=[0,plot_df[pcey_obj.COL_DAILY_IDEAL_YIELD].max()*1.1])
        fig.update_xaxes(range=[0,plot_df[pcey_obj.COL_DAILY_PREDICTED].max()*1.1])

        
        # set the legend to be 'h'
        fig.update_layout(legend_orientation="h")
        # center the legend
        fig.update_layout(legend=dict(x=0.5, y=-0.1, xanchor='center', yanchor='top'))
        # set the height to be 800
        # fig.update_layout(height=1500)
        # set the max width to be 1000
        # remove padding from the plot
        fig.update_layout(margin=dict(l=0, r=0))

        # plotl white themw
        fig.update_layout(template='plotly_white')
        title = f"""<span style='font-size: 16; font-weight: bold;'>{pcey_obj.name}, {pcey_obj.bmu}</span><br><span style='font-size: 14px;'>{pcey_obj.n_data_points} months, r-squared: {pcey_obj.prediction_r2:.2f}</span>
        <br><span style='font-size: 12px;'>The auto-qc process has removed months with a data coverage of less than 75%.</span>"""
        # title = f'{self.name}, {self.bmu} scatter. Prediction v Actual'
        fig.update_layout(title_text=title, title_x=0.5, title_font_size=16)
        fig.update_layout(
            dragmode=False)
                # reduce space between subplots
        # turn off 


        # save the plot
        fig.write_html(f"{plot_path}.html", full_html=False,config={'displayModeBar': False, 'displaylogo': False})



    except Exception as e:
        line_number = sys.exc_info()[-1].tb_lineno
        print(f"plot_scatter(), error: {e}, line: {line_number}")




@staticmethod
def plot_p50(pcey_obj):
    try:
        # make a folder for the plots
        plot_folder = os.path.join(project_root_path, 'plots')
        if not os.path.exists(plot_folder):
            os.mkdir(plot_folder)
        filename = f'3_{pcey_obj.bmu}_p50'
        plot_path = os.path.join(plot_folder, filename)

        fig = go.Figure()
        text = [f"{expected:.2f} GWh<br>Month: {month}" for expected, month in zip(pcey_obj.energy_yield_df['energy_without_curtailment'], pcey_obj.energy_yield_df['month_name'])]
        fig.add_trace(go.Bar(x=pcey_obj.energy_yield_df.index-0.15,width=0.3, y = pcey_obj.energy_yield_df['energy_without_curtailment'], name='Expected (without curtailment losses)',
                            hovertext=text,hoverinfo='text'))
        text = [f"{expected:.2f} GWh<br>Month: {month}" for expected, month in zip(pcey_obj.energy_yield_df['energy_with_curtailment'], pcey_obj.energy_yield_df['month_name'])]
        fig.add_trace(go.Bar(x=pcey_obj.energy_yield_df.index+0.15,width=0.3,y= pcey_obj.energy_yield_df['energy_with_curtailment'], name='Expected (with curtailment losses)',
                            hovertext=text,hoverinfo='text'))
        # make the ticks 'Jan', 'Feb' as opposed to 1, 2
        labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        # add the labels to the x axis
        fig.update_xaxes(ticktext=labels, tickvals=pcey_obj.energy_yield_df.index)
        fig.update_layout(legend_orientation="h")
        # add y axis labels
        fig.update_yaxes(title_text='Monthly Energy Yield (GWh)')
        # center the legend
        fig.update_layout(legend=dict(x=0.5, y=-0.1, xanchor='center', yanchor='top'))
        # set the height to be 800
        fig.update_layout(height=600)
        # set the max width to be 1000
        # remove padding from the plot
        fig.update_layout(margin=dict(l=0, r=0))

        # plotl white themw
        fig.update_layout(template='plotly_white')
        # plotly latex r2
        title=f"""<b style = 'font-size:16px'>{pcey_obj.name} Annual Energy Yield</b><br>
        <span style='font-size:14px'>without curtailment: {pcey_obj.p50_ideal_yield:.0f} GWh, with curtailment: {pcey_obj.p50_energy_yield:.0f} GWh</span><br>
        <span style='font-size:12px'>BMU: {pcey_obj.bmu}, number of months: {pcey_obj.n_data_points}, model r-squared: {pcey_obj.prediction_r2:.2f}, losses due to curtailment: {pcey_obj.losses_as_percentage:.2f}%</span>"""
        fig.update_layout(title_text=title, title_x=0.5, title_font_size=16)
        fig.update_layout(
            dragmode=False)
                # reduce space between subplots

        fig.write_html(f"{plot_path}.html", full_html=False,config={'displayModeBar': False, 'displaylogo': False})
    
    except Exception as e:
        line_number = sys.exc_info()[-1].tb_lineno
        print(f"plot_p50(), error: {e}, line: {line_number}")


