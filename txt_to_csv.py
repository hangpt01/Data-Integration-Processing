import json
import csv

# Define the input and output file paths
input_file = 'Dataset/IMDB/testing_data.txt'
output_file = 'Dataset/IMDB/csv/testing_data.csv'

# Open the input file and read the lines
with open(input_file, 'r', encoding='utf-8') as file:
    lines = file.readlines()

# Open the output CSV file for writing
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['review_id', 'movie', 'rating', 'review_date', 'release_time']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    # Write the header row
    writer.writeheader()

    # Convert each line from JSON to a dictionary and write to CSV
    for line in lines:
        review = json.loads(line)
        writer.writerow(review)

print(f'Conversion complete. The CSV file has been saved as {output_file}.')
