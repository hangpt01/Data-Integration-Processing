import pandas as pd
from sklearn.model_selection import train_test_split

# df = pd.read_csv("Dataset/IMDB/csv/part-01.csv")
df = pd.read_csv("Dataset/IMDB/csv/testing_data.csv")

# Count occurrences of each movie
movie_counts = df['movie'].value_counts()

# Filter for movies with at least 100 reviews
movies_with_100_reviews = movie_counts[movie_counts >= 100].index

# Filter the original DataFrame to keep only rows with these movies
filtered_df = df[df['movie'].isin(movies_with_100_reviews)]

# Reindex the DataFrame
filtered_df = filtered_df.reset_index(drop=True)

# Split the data into training and testing sets
train_df, test_df = train_test_split(filtered_df, test_size=0.2, random_state=42)

# # Save the training set to a CSV file
# train_df.to_csv('Dataset/IMDB/csv/training_set.csv', index=False)
# # Save the testing set to a CSV file
# test_df.to_csv('Dataset/IMDB/csv/testing_set.csv', index=False)

# Save the training set to a CSV file
train_df.to_csv('Dataset/IMDB/training_set.csv', index=False)
# Save the testing set to a CSV file
test_df.to_csv('Dataset/IMDB/testing_set.csv', index=False)

print("All files have been processed.")

