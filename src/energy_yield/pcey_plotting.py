import os
import plotly.graph_objects as go
import matplotlib.pyplot as plt


# change the matplotlib font to use open sans
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = 'Open Sans'

COLOURS = ['#3F7D20', '#A0DDE6', '#542e71','#3F7CAC','#698F3F']



class Plottter:
    def __init__(self, pcey_obj):
        self.pcey_obj = pcey_obj



    def plot_generation(self):
        try:
            root = os.path.dirname(os.path.abspath(__file__))
            # make a folder for the plots
            plot_folder = os.path.join(root, 'plots')
            if not os.path.exists(plot_folder):
                os.mkdir(plot_folder)

            filename = f'2_{self.bmu}_pcey'
            plot_path = os.path.join(plot_folder, filename)
            plot_df = self.weather_stats_df.copy()


            fig = go.Figure()
            fig.add_trace(go.Scattergl(x=plot_df.index, y=plot_df[self.COL_PREDICTED_MONTHLY], name='Predicted',mode='lines+markers'))
            fig.add_trace(go.Scattergl(x=plot_df.index, y=plot_df[self.COL_IDEAL_YIELD], name='Generation - Curtailment', mode='lines+markers'))
            fig.add_trace(go.Scattergl(x=plot_df.index, y=plot_df[self.COL_NET_YIELD], name='Generation', mode='lines+markers'))
            title = f"<span style='font-size: 20px; font-weight: bold;'>{self.name}</span><br><span style='font-size: 16px;'>{self.bmu}</span>"
            fig.update_layout(title=title, xaxis_title='Month', yaxis_title='GWh')
            # whit theme
            fig.update_layout(template='plotly_white')
            # save to html
            fig.write_html(f'{plot_path}.html', full_html=False,config={'displayModeBar': False, 'displaylogo': False})

            plt.close()
        except Exception as e:
            print('plot_generation(), error: ', e)


    def plot_scatter(self):
        try:
            root = os.path.dirname(os.path.abspath(__file__))
            # make a folder for the plots
            plot_folder = os.path.join(root, 'plots')
            if not os.path.exists(plot_folder):
                os.mkdir(plot_folder)
            filename = f'1_{self.bmu}_scatter'
            plot_path = os.path.join(plot_folder, filename)


            # three column, 1 row subplots with Go
            from plotly.subplots import make_subplots
            colours = COLOURS
            
            #subplot 1
            fig = make_subplots(rows=3, cols=1, 
                                subplot_titles=(f"Step 1. Actual GWh v Weather Data (ERA5 Node: lat: {self.nearest_lat:.2f}, lon: {nearest_lon:.2f})", f"Step 2. (Actual GWh - Curtailed GWh) v Weather Data", f"Step 3. Predicted GWh v Actual GWh", ), 
                                vertical_spacing=0.1)           
            fig.update_annotations(font_size=12)    
            # the wind speed, production and the month in text
            ok_text = [f"QC pass<br>Wind Speed: {ws:.2f} m/s<br>Production: {prod:.2f} GWh<br>Month: {month}" for ws, prod, month in zip(self.weather_stats_df['wind_speed'], self.weather_stats_df[self.COL_DAILY_NET_YIELD], self.weather_stats_df.index.strftime('%b %Y'))]
            fig.add_trace(go.Scatter(x=self.weather_stats_df['wind_speed'], y=self.weather_stats_df[self.COL_DAILY_NET_YIELD], mode='markers', name='Auto QC pass', marker_color=colours[2],legendgroup='1',
                                    hovertext=ok_text,hoverinfo='text'), row=1, col=1)
            fail_text = [f"QC fail<br>Wind Speed: {ws:.2f} m/s<br>Production: {prod:.2f} GWh<br>Month: {month}" for ws, prod, month in zip(self.weather_stats_df['wind_speed'], self.weather_stats_df[self.COL_DAILY_NET_YIELD + '_fail'], self.weather_stats_df.index.strftime('%b %Y'))]
            fig.add_trace(go.Scatter(x=self.weather_stats_df['wind_speed'], y=self.weather_stats_df[self.COL_DAILY_NET_YIELD + '_fail'], mode='markers', name='Auto QC fail', marker_color=colours[4], opacity=0.5, legendgroup='1',
                                        hovertext=fail_text,hoverinfo='text'), row=1, col=1)
            fig.update_xaxes(title_text='Daily Mean Wind Speed (m/s)', row=1, col=1,titlefont=dict(size=12))
            fig.update_yaxes(title_text='Actual Daily Mean Generation (GWh)', row=1, col=1,titlefont=dict(size=12))
            # make the r^2 text

            fig.update_yaxes(range=[0,self.weather_stats_df[self.COL_DAILY_IDEAL_YIELD].max()*1.1], row=1, col=1)
            fig.update_xaxes(range=[0,self.weather_stats_df['wind_speed'].max()*1.1], row=1, col=1)


            
            #subplot 2
            ok_text = [f"QC pass<br>Wind Speed: {ws:.2f} m/s<br>Production: {prod:.2f} GWh<br>Month: {month}" for ws, prod, month in zip(self.weather_stats_df['wind_speed'], self.weather_stats_df[self.COL_DAILY_IDEAL_YIELD], self.weather_stats_df.index.strftime('%b %Y'))]
            fig.add_trace(go.Scatter(x=self.weather_stats_df['wind_speed'], y=self.weather_stats_df[self.COL_DAILY_IDEAL_YIELD], mode='markers', name='Auto QC pass', marker_color=colours[2],legendgroup='2'
                                        ,hovertext=ok_text,hoverinfo='text', showlegend=False), row=2, col=1)
            fail_text = [f"QC fail<br>Wind Speed: {ws:.2f} m/s<br>Production: {prod:.2f} GWh<br>Month: {month}" for ws, prod, month in zip(self.weather_stats_df['wind_speed'], self.weather_stats_df[self.COL_DAILY_IDEAL_YIELD + '_fail'], self.weather_stats_df.index.strftime('%b %Y'))]
            fig.add_trace(go.Scatter(x=self.weather_stats_df['wind_speed'], y=self.weather_stats_df[self.COL_DAILY_IDEAL_YIELD + '_fail'], mode='markers', name='Auto QC fail', marker_color=colours[4], opacity=0.5, legendgroup='2',
                                        hovertext=fail_text,hoverinfo='text', showlegend=False), row=2, col=1)

            if self.intercept > 0:
                text = f'y = {self.slope:.2f}x + {abs(self.intercept):.2f}'
            else:
                text = f'y = {self.slope:.2f}x - {abs(self.intercept):.2f}'
            x_range = np.arange(0, self.weather_stats_df['wind_speed'].max()*1.1, 0.1)
            fig.add_trace(go.Scatter(x=x_range, y=self.slope * x_range + self.intercept, mode='lines', name=text, line=dict(color=colours[3], dash='dash')), row=2, col=1)
            fig.update_xaxes(title_text='Daily Mean Wind Speed (m/s)', row=2, col=1,titlefont=dict(size=12))
            fig.update_yaxes(title_text='Actual Daily Mean Generation (GWh) - Curtailed GWh', row=2, col=1,titlefont=dict(size=12))
            fig.update_yaxes(range=[0,self.weather_stats_df[self.COL_DAILY_IDEAL_YIELD].max()*1.1], row=2, col=1)
            fig.update_xaxes(range=[0,self.weather_stats_df['wind_speed'].max()*1.1], row=2, col=1)

            #subplot 3
            text = [f"Predicted: {pred:.2f} GWh<br>Actual: {actual:.2f} GWh<br>Month: {month}" for pred, actual, month in zip(self.weather_stats_df[self.COL_DAILY_PREDICTED], self.weather_stats_df[self.COL_DAILY_IDEAL_YIELD], self.weather_stats_df.index.strftime('%b %Y'))]
            fig.add_trace(go.Scatter(x=self.weather_stats_df[self.COL_DAILY_PREDICTED], y=self.weather_stats_df[self.COL_DAILY_IDEAL_YIELD], mode='markers',marker_color = colours[2],legendgroup='3',
                                        hovertext=text,hoverinfo='text', showlegend=False), row=3, col=1)
            _lin_reg_df = self.weather_stats_df[[self.COL_DAILY_PREDICTED, self.COL_DAILY_IDEAL_YIELD]].dropna()
            # linear regression between predicted and actual
            slope, intercept, r_value, p_value, ideal_std_err = stats.linregress(_lin_reg_df[self.COL_DAILY_PREDICTED], _lin_reg_df[self.COL_DAILY_IDEAL_YIELD])
            x_range = np.arange(0, self.weather_stats_df[self.COL_DAILY_PREDICTED].max()*1.1, 0.1)
            r2_text = f'R^2 = {r_value**2:.2f}'

            fig.add_trace(go.Scatter(x=x_range, y=slope * x_range + intercept, mode='lines', name=r2_text, line=dict(color='red', dash='dash')), row=3, col=1)
            fig.update_xaxes(title_text='Predicted Daily Mean Generation (GWh)', row=3, col=1 ,titlefont=dict(size=12))
            fig.update_yaxes(title_text='Actual Daily Mean Generation (GWh)', row=3, col=1,titlefont=dict(size=12))
            fig.update_yaxes(range=[0,self.weather_stats_df[self.COL_DAILY_IDEAL_YIELD].max()*1.1], row=3, col=1)
            fig.update_xaxes(range=[0,self.weather_stats_df[self.COL_DAILY_PREDICTED].max()*1.1], row=3, col=1)

            
            # set the legend to be 'h'
            fig.update_layout(legend_orientation="h")
            # center the legend
            fig.update_layout(legend=dict(x=0.5, y=-0.1, xanchor='center', yanchor='top'))
            # set the height to be 800
            fig.update_layout(height=1500)
            # set the max width to be 1000
            # remove padding from the plot
            fig.update_layout(margin=dict(l=0, r=0))

            # plotl white themw
            fig.update_layout(template='plotly_white')
            title = f"<span style='font-size: 16; font-weight: bold;'>{self.name}, {self.bmu}<br>Least squares model</span>"
            # title = f'{self.name}, {self.bmu} scatter. Prediction v Actual'
            fig.update_layout(title_text=title, title_x=0.5, title_font_size=16)
            fig.update_layout(
                dragmode=False)
                    # reduce space between subplots
            # turn off 


            # save the plot
            fig.write_html(f"{plot_path}.html", full_html=False,config={'displayModeBar': False, 'displaylogo': False})


            


        except Exception as e:
            print('plot_scatter(), error: ', e)




    def plot_p50(self):
        try:
            root = os.path.dirname(os.path.abspath(__file__))
            # make a folder for the plots
            plot_folder = os.path.join(root, 'plots')
            if not os.path.exists(plot_folder):
                os.mkdir(plot_folder)
            filename = f'3_{self.bmu}_p50'
            plot_path = os.path.join(plot_folder, filename)
            fig = plt.figure(figsize=(10,5))
            ax = fig.add_subplot(111)
            
            ax.bar(self.energy_yield_df.index-0.15,width=0.3, height = self.energy_yield_df[self.COL_PREDICTED_MONTHLY], label='Predicted (without curtailment losses)')

            ax.bar(self.energy_yield_df.index+0.15,width=0.3,height= self.energy_yield_df['net_yield_GWh'], label='Predicted (with curtailment losses)')
            # make the ticks 'Jan', 'Feb' as opposed to 1, 2
            labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            ax.legend()
            ax.set_xticks(self.energy_yield_df.index)
            ax.set_xticklabels(labels)

            ax.set_xlabel('Month')
            ax.set_ylabel('P50 Energy Yield (GWh)')
            # r2 text

            fig.suptitle(f'{self.bmu}, Capacity {self.capacity:.0f}MW Annual P50 Energy Yield: {self.p50_energy_yield:.0f} GWh, number of months: {self.n_data_points}\nModel $r^2$: {self.prediction_r2:.2f}')
            self.losses_as_percentage = f'{self.losses_due_to_curtailment / self.p50_energy_yield * 100:.2f}%'
            losses_text = f'Annual observed losses due to curtailment: {self.losses_due_to_curtailment:.0f} GWh {self.losses_as_percentage}'
            ax.set_title(losses_text)

            fig.savefig(f"{plot_path}.png")
            plt.close()
            fig = go.Figure()
            fig.add_trace(go.Bar(x=self.energy_yield_df.index-0.15,width=0.3, y = self.energy_yield_df[self.COL_PREDICTED_MONTHLY], name='Predicted (without curtailment losses)'))
            fig.add_trace(go.Bar(x=self.energy_yield_df.index+0.15,width=0.3,y= self.energy_yield_df['net_yield_GWh'], name='Predicted (with curtailment losses)'))
            # make the ticks 'Jan', 'Feb' as opposed to 1, 2
            labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            # add the labels to the x axis
            fig.update_xaxes(ticktext=labels, tickvals=self.energy_yield_df.index)
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
            title=f"""<b style = 'font-size:16px'>BMU: {self.bmu}<br>Annual Energy Yield: {self.p50_energy_yield:.0f} GWh</b>
            <br><span style='font-size:14px'>number of months: {self.n_data_points}, model r-squared: {self.prediction_r2:.2f}</span>"""
            fig.update_layout(title_text=title, title_x=0.5, title_font_size=16)
            fig.update_layout(
                dragmode=False)
                    # reduce space between subplots

            fig.write_html(f"{plot_path}.html", full_html=False,config={'displayModeBar': False, 'displaylogo': False})
        
        except Exception as e:
            print('plot_p50(), error: ', e)


