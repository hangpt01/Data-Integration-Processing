import json
from kafka import KafkaProducer
import numpy as np

# Set up the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def process_line(line, keys_to_keep, defaults):
    data = json.loads(line)
    filtered_data = {}
    for key in keys_to_keep:
        value = data.get(key)
        # Check if value is None or NaN (for numeric values), replace with default if so
        if value is None or (isinstance(value, (float, int)) and np.isnan(value)):
            filtered_data[key] = defaults.get(key, None)
        else:
            filtered_data[key] = value
    return filtered_data

# Default values for each key
default_values = {
    'videos': '', 'details': '', 'description': '', 'store': '', 
    'images': '', 'rating_number': 0, 'title': '', 'bought_together': '', 
    'parent_asin': '', 'price': 0.0, 'main_category': '', 'features': '', 
    'categories': '', 'average_rating': 0.0
}


# Specify the keys to keep
keys_to_keep = {'store', 'rating_number', 'title', 'parent_asin', 'price', 'main_category', 'average_rating'}

# Path to the JSONL file
jsonl_file_path = 'data/amazon/meta_Automotive.jsonl'

# books, all beauty 
# Read and process the JSONL file line by line
try:
    with open(jsonl_file_path, 'r') as file:
        for line in file:
            try:
                filtered_record = process_line(line, keys_to_keep, default_values)
                producer.send('amazon-products4', value=filtered_record)
                print(f"Sent 1 item {filtered_record}")
            except json.JSONDecodeError:
                print(f"Error decoding JSON from line: {line}")
            except Exception as e:
                print(f"Failed to send record due to {e}")
    
    # Ensure all messages are sent
    producer.flush()
    print("All records have been sent to Kafka topic 'amazon-products'")

except FileNotFoundError:
    print(f"File not found: {jsonl_file_path}")
except Exception as e:
    print(f"An error occurred: {e}")

# Close the producer
producer.close()