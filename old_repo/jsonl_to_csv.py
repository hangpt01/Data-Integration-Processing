import json
import csv
import os

def merge_jsonl_files(input_directory, output_file):
    with open(output_file, 'w') as outfile:
        for filename in os.listdir(input_directory):
            if filename.endswith('.jsonl'):
                file_path = os.path.join(input_directory, filename)
                with open(file_path, 'r') as infile:
                    for line_number, line in enumerate(infile, start=1):
                        try:
                            json_obj = json.loads(line)
                            json.dump(json_obj, outfile)
                            outfile.write('\n')
                        except json.decoder.JSONDecodeError as e:
                            print(f"Error decoding JSON in file {filename} on line {line_number}: {e}")
                            print(f"Problematic line: {line}")
    print(f"All JSONL files merged into {output_file}")

# Specify the directory containing JSONL files and the output file
input_directory = 'data_amazon'
output_file = 'merge_data/amazon_multiple.jsonl'

merge_jsonl_files(input_directory, output_file)


# Open the JSONL file
with open('merge_data/amazon_multiple.jsonl', 'r', encoding='utf-8') as json_file:
    # Read all lines from the file
    lines = json_file.readlines()

# Parse each line as JSON
data = [json.loads(line) for line in lines]

# Define the CSV file name
csv_file_name = 'merge_data/merged_output_amazon.csv'

# Extract the keys (column names) from the first item in the JSON data
keys = data[0].keys()

# Write the JSON data to a CSV file
with open(csv_file_name, 'w', newline='', encoding='utf-8') as csv_file:
    dict_writer = csv.DictWriter(csv_file, fieldnames=keys)
    dict_writer.writeheader()
    dict_writer.writerows(data)

print(f"JSON data has been written to {csv_file_name}")
