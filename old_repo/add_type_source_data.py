import json
import re
import os

input_folder_path = "data"
output_folder_path = "processed_data"
output_merged_path = "merge_data"

json_files = [f for f in os.listdir(input_folder_path) if f.endswith('.json')]
    
def clean_filename(filename):
    # Remove numbers and special characters, keep only alphabets and spaces
    cleaned_name = re.sub(r'[^A-Za-z\s]', '', filename)
    return cleaned_name.strip()
    
# Function to extract the source from the URL
def extract_source(url):
    if url:
        match = re.search(r'www\.(.*?)\.', url)
        if match:
            return match.group(1)
    return None

all_filtered_items = []

for file_name in json_files:
    file_path = os.path.join(input_folder_path, file_name)

    # Read the input JSON file
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    item_type = clean_filename(os.path.splitext(file_name)[0])
    # item_type = re.sub(r'\d+', '', os.path.splitext(file_name)[0])
    # Add the new fields to each item
    for item in data:
        item['item_type'] = item_type
        item['source'] = extract_source(item.get('url'))

    # Filter items with null price and null source
    filtered_items = [item for item in data if item.get('price') is not None and item.get('source') is not None]
    all_filtered_items.extend(filtered_items)

    # Write the filtered data to a new JSON file
    if not os.path.exists(output_folder_path):
        os.makedirs(output_folder_path)
    output_file_path = os.path.join(output_folder_path,file_name)
    with open(output_file_path, 'w', encoding='utf-8') as file:
        json.dump(filtered_items, file, ensure_ascii=False, indent=4)

    print("Filtered data has been written to {}.json".format(output_file_path))

if not os.path.exists(output_merged_path):
        os.makedirs(output_merged_path)

output_merged_file = os.path.join(output_merged_path,"merged_output.json")
with open(output_merged_file, 'w', encoding='utf-8') as merged_file:
    json.dump(all_filtered_items, merged_file, ensure_ascii=False, indent=4)

print(f"All filtered data has been merged into {output_merged_file}")