import json
from kafka import KafkaProducer

# Function to read JSON file
def read_json_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

# Set up the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Read JSON data from file
json_file_path = 'merge_data/merged_output.json'
json_data = read_json_file(json_file_path)
# breakpoint()
# Send each JSON object to Kafka topic
for record in json_data:
    producer.send('lazada-products', value=record)
    producer.flush()  # Ensure each message is sent
    print("Sent 1 item")
print(f"{len(json_data)} data has been sent to Kafka topic streaming-data")

# Close the producer
producer.close()
