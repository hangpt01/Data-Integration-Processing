import json
from kafka import KafkaProducer

# Set up the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Path to the large JSON file
json_file_path = "archive2/part-01.json"

# Kafka topic name
topic_name = "mubi-sqlite"

# Function to read large JSON file and send each line to Kafka
def send_json_to_kafka(file_path, topic):
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            try:
                # Parse each line as JSON
                data = json.loads(line.strip())
                for d in data:
                # breakpoint()
                # Send the JSON data to Kafka
                    producer.send(topic, value=d)
                    producer.flush()
                    print("sent 1 item")
            except json.JSONDecodeError as e:
                print(f"Failed to decode JSON: {e}")
            except Exception as e:
                print(f"Failed to send data to Kafka: {e}")

    # Ensure all messages are sent before closing the producer
    

# Call the function to send data to Kafka
send_json_to_kafka(json_file_path, topic_name)