import pandas as pd
from sklearn.model_selection import train_test_split

# df = pd.read_csv("Dataset/IMDB/csv/part-01.csv")
df = pd.read_csv("Dataset/The_mother_of_all_movie/user_reviews.csv/user_reviews.csv")

# Count occurrences of each movie
movie_counts = df['movie'].value_counts()

# Filter for movies with at least 100 reviews
movies_with_100_reviews = movie_counts[movie_counts >= 100].index

# Filter the original DataFrame to keep only rows with these movies
filtered_df = df[df['movie'].isin(movies_with_100_reviews)]

# Reindex the DataFrame
filtered_df = filtered_df.reset_index(drop=True)

filtered_df.to_csv('Dataset/The_mother_of_all_movie/data_tomatoes.csv', index=False)

print("All files have been processed.")

