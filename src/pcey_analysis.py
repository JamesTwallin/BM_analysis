import ast
import os
from src.utils import helpers
import datetime as dt
import textwrap

global project_root_path
project_root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(project_root_path)

def enforce_list(_string_list):
	try:
		assert isinstance(_string_list, list)
		return _string_list
	except:
		string_list = ast.literal_eval(_string_list)
		return string_list

def append_html_to_md(windfarm_df):
    # with open(md_file_path, 'a') as md_file:

    # your code to write data

        for index, row in windfarm_df.head(10).iterrows():
            date_string = dt.datetime.now().strftime("%Y-%m-%d")
            name = row['name']
            md_file_path = os.path.join(project_root_path, 'docs', '_windfarms', f"{date_string}-{name.lower().replace(' ', '_')}.md")
            with open(md_file_path, 'w', encoding='utf-8') as md_file:
                try:
                    name = row['name']
                    # Add BMU name as a header with a proper utf-8 encoding

                    # add this:

                    text = f'''
                    ---
                    title: {name}
                    author: James Twallin
                    date: {date_string}
                    category: Jekyll
                    layout: windfarm
                    ---
                    '''

                    text = textwrap.dedent(text)
                    # remove the first line
                    text = text.split('\n', 1)[1]

                    bmus = enforce_list(row['bmrs_id'])
                    
                    for bmu in bmus:
                        # list dir with a wildcard
                        file_list = os.listdir(os.path.join(project_root_path, 'docs', 'assets',))
                        file_list = [file for file in file_list if bmu in file and 'png' in file]
                        for file in file_list:
                            file_path = os.path.join(project_root_path, 'plots', file)
                            # check if the file exists
    

                            # tjis is the png file
                            # add the png file to the markdown file
                            # ![]({{ site.baseurl }}/assets/{}.png)
                            # make sure it is double curly brackets

                            
                            text += ("""![]({{ site.baseurl }}""" + f"/assets/{file})\n")

                            # put the png file in the assets folder
                            # move the png file to the assets folder


                            
                        md_file.write(text)
                # got to the plots folder and get the html files with contain any of the BMU names
                
                # bmu_contents = bmu_file.read()
                # # Append the HTML content
                # md_file.write(f"{bmu_contents}\n")
                except Exception as e:
                    print(f"An error occurred while processing BMU {name}: {e}")

# List of BMUs

if __name__ == "__main__":
    windfarm_df = helpers.read_custom_windfarm_csv()

    # Path to your markdown file
    append_html_to_md(windfarm_df)