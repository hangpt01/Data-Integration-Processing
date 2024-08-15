import json
from kafka import KafkaProducer


# Set up the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# csv_file_path = ""
import pandas as pd
chunk_size = 10000
columns_list = ['review_id', 'movie', 'rating', 'review_date', 'release_time']

## read data
df = pd.read_csv("Dataset/IMDB/csv/testing_set.csv",chunksize=chunk_size, usecols=columns_list)
topic_name = "imdb"

for chunk_df in df:
    for _, row in chunk_df.iterrows():
        row_dict = row.to_dict()
        producer.send(topic_name, value=row_dict)
        producer.flush()
        print("sent item")


producer.close()
