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
columns_list = ['movieId', 'rating','reviewId', 'isVerified','score','userDisplayName', 'userRealm', 'userId']

## read data
df = pd.read_csv("archive/user_reviews.csv/user_reviews.csv",chunksize=chunk_size, usecols=columns_list)
topic_name = "rotten-tomatoes"

for chunk_df in df:
    for _, row in chunk_df.iterrows():
        row_dict = row.to_dict()
        producer.send(topic_name, value=row_dict)
        producer.flush()
        print("sent item")

producer.close()
