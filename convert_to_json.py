import pandas as pd

# Read the CSV file
df = pd.read_csv('pcey.csv')
for col in df.columns:
    df.rename(columns={col: col.replace(" ", "_")}, inplace=True)

# Convert the DataFrame to JSON
json_data = df.to_json(orient='records')

# Print the JSON data
with open('pcey.json', 'w') as f:
    f.write(json_data)
