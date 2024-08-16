import os

# Set the path to your large CSV file and the output file
input_file_path = "Dataset/The_mother_of_all_movie/user_reviews.csv/user_reviews.csv"
output_file_path = "Dataset/The_mother_of_all_movie/user_reviews.csv/user_reviews_small.csv"

# Set the target size (500MB)
target_size = 500 * 1024 * 1024  # 500 MB in bytes

# Initialize variables to keep track of the size of data written
current_size = 0

# Open the input and output files
with open(input_file_path, "r", encoding="utf-8") as infile, open(output_file_path, "w", encoding="utf-8") as outfile:
    # Read and write the header (first line) of the CSV file
    header = infile.readline()
    outfile.write(header)
    
    # Iterate through the lines of the file
    for line in infile:
        # Check the size of the current line
        line_size = len(line.encode("utf-8"))
        
        # If adding this line exceeds the target size, stop writing
        if current_size + line_size > target_size:
            break
        
        # Write the current line to the output file
        outfile.write(line)
        
        # Update the current size
        current_size += line_size

print(f"First {current_size/(1024*1024):.2f} MB of data written to {output_file_path}.")
