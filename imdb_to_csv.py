import json
import csv
import os
from datetime import datetime
import re

# Define the folder containing the JSON files
input_folder = './Dataset/IMDB'
output_folder = './Dataset/IMDB/csv/'

# Define the list of files to process
file_names = [f"part-0{i}.json" for i in range(1, 7)]

# Define the fields to keep, including the new field 'release_time'
fields_to_keep = ["review_id", "movie", "rating", "review_date", "release_time"]

def convert_date_format(date_str):
    try:
        # Parse the date from the original format
        parsed_date = datetime.strptime(date_str, "%d %B %Y")
        # Convert to the desired format
        return parsed_date.strftime("%Y-%m-%d")
    except ValueError:
        # Return the original date if parsing fails
        return date_str

def extract_movie_name_and_year(movie_str):
    # Regex to handle standard formats and complex cases with episodes
    match = re.match(r"^(.*?)(?::\s*.*)?\s*\((\d{4})(?:–\d{4})?(?:–\s*)?\)", movie_str)
    if match:
        movie_name = match.group(1).strip()  # Extract the main movie/show title
        release_year = match.group(2)  # Extract the year (start year in case of a range)
        return movie_name, release_year
    
    # If no match, return the original string as movie name with None for release year
    return movie_str, None

# Loop through each file
for file_name in file_names:
    input_json_file = os.path.join(input_folder, file_name)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    output_csv_file = os.path.join(output_folder, file_name.replace('.json', '.csv'))
    
    # Load JSON data from the file
    with open(input_json_file, 'r', encoding='utf-8') as file:
        json_data = json.load(file)
    
    # Open CSV file for writing
    with open(output_csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fields_to_keep)
        
        # Write the header
        writer.writeheader()
        
        # Write the rows
        for review in json_data:
            movie_name, release_year = extract_movie_name_and_year(review['movie'])
            filtered_review = {
                "review_id": review["review_id"],
                "movie": movie_name,
                "rating": review["rating"],
                "review_date": convert_date_format(review["review_date"]),
                "release_time": release_year
            }
            writer.writerow(filtered_review)
    
    print(f"CSV file '{output_csv_file}' has been created successfully.")

print("All files have been processed.")
