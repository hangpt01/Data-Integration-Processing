import json
from kafka import KafkaProducer


# Set up the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
# csv_file_path = ""
import pandas as pd
path_dir = f"archive"
movies_df = pd.read_csv(f"{path_dir}/movies.csv/movies.csv")


chunk_size = 10000
columns_list = ['movieId', 'rating','reviewId', 'isVerified','score','userDisplayName', 'userRealm', 'userId']

## read data
df = pd.read_csv(f"{path_dir}/user_reviews.csv/user_reviews.csv",chunksize=chunk_size, usecols=columns_list)
topic_name = "rotten-tomatoes"

import tqdm
for chunk_df in tqdm.tqdm(df):
    for _, row in tqdm.tqdm(chunk_df.iterrows()):
        try:
            row_dict = row.to_dict()
            movies_df[movies_df['movieId'] == row_dict['movieId']]

            movie_info = movies_df[movies_df['movieId'] == row_dict['movieId']]
            row_dict['movieYear'] = str(movie_info['movieYear'].values[0])
            row_dict['movieTitle'] = movie_info['movieTitle'].values[0]
            # row_dict['movieTitle'] = movie_info['movieTitle'].values[0]
            # breakpoint()
            producer.send(topic_name, value=row_dict)
            producer.flush()
            # print("sent item")
        except:
            pass

producer.close()
