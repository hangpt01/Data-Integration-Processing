import pandas as pd
import json

# Load the JSON data
with open('Dataset/IMDB/testing_data.json', 'r') as json_file:
    data = json.load(json_file)

# If the JSON data is a list of dictionaries (like most JSON files)
if isinstance(data, list):
    df = pd.DataFrame(data)
else:
    # If the JSON data is a single dictionary
    df = pd.json_normalize(data)

# Save the DataFrame to a CSV file
df.to_csv('Dataset/IMDB/csv/testing_data.csv', index=False)

print("JSON file has been converted to CSV and saved as 'Dataset/IMDB/csv/testing_data.csv'")
