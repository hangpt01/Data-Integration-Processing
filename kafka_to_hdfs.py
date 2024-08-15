from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import os

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,  # Disable auto commit for better control over offset
}

# Function to append the batch data to a JSON file
def append_to_json_file(file_path, data_batch):
    try:
        # Check if the file exists and read the existing content
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                content = json.load(f)
        else:
            content = []

        # Append the new data
        content.extend(data_batch)
        
        # Write back the updated content as a JSON array
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(content, f, indent=2)
    
    except Exception as e:
        print(f"Error while writing to JSON file: {e}")
        raise

def consume_kafka_to_json(topic, json_file_path, batch_size=100):
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    data_batch = []
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    raise KafkaException(msg.error())

            # Process message
            record_value = msg.value().decode('utf-8')
            data = json.loads(record_value)
            data_batch.append(data)

            # When batch size is reached, write to the JSON file
            if len(data_batch) >= batch_size:
                append_to_json_file(json_file_path, data_batch)
                data_batch.clear()
                consumer.commit()  # Commit offsets after successful write

    except KeyboardInterrupt:
        pass
    finally:
        # Write any remaining data in the batch to the JSON file
        if data_batch:
            append_to_json_file(json_file_path, data_batch)
        consumer.close()

if __name__ == "__main__":
    topic = 'imdb'
    json_file_path = '/user/Asus/testing_data.txt'  # Ensure this path is correct
    consume_kafka_to_json(topic, json_file_path)
