from confluent_kafka import Consumer, KafkaException, KafkaError
from hdfs import InsecureClient, HdfsError
import json
import argparse

# Kafka consumer configuration
kafka_conf = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

# HDFS configuration (using the service name 'namenode')
hdfs_client = InsecureClient('http://localhost:9870', user='hdfs')

def append_to_hdfs(client, path, data):
    try:
        # Read the existing content
        try:
            with client.read(path, encoding='utf-8') as reader:
                content = json.load(reader)
        except HdfsError:
            content = []

        # Append the new data
        content.append(data)
        
        # Write back the updated content
        client.write(path, data=json.dumps(content, indent=2), encoding='utf-8', overwrite=True)

    except HdfsError as e:
        print(f"Error while appending to HDFS: {e}")
        raise

def consume_kafka_to_hdfs(topic, hdfs_path):
    consumer = Consumer(kafka_conf)
    consumer.subscribe([topic])

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
            record_key = msg.key().decode('utf-8') if msg.key() else None
            record_value = msg.value().decode('utf-8')
            data = {
                'key': record_key,
                'value': record_value
            }

            # Append to HDFS
            append_to_hdfs(hdfs_client, hdfs_path, data)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    # Argument parsing
    parser = argparse.ArgumentParser(description="Consume Kafka messages and save to HDFS for Amazon or Lazada.")
    parser.add_argument('source', type=str, choices=['amazon', 'lazada'], help='Specify the data source: "amazon" or "lazada".')
    
    args = parser.parse_args()

    # Define the file mappings
    file_mappings = {
        'amazon': {
            'train_file_path': 'hdfs://localhost:9000/user/pth/train_amazon.csv',
            'test_file_path': 'hdfs://localhost:9000/user/pth/test_amazon.csv',
            'kafka_topic': 'amazon-products'
        },
        'lazada': {
            'train_file_path': 'hdfs://localhost:9000/user/pth/train_lazada.csv',
            'test_file_path': 'hdfs://localhost:9000/user/pth/test_lazada.csv',
            'kafka_topic': 'lazada-products'
        }
    }

    # Get the file paths and Kafka topic based on the source
    if args.source in file_mappings:
        paths = file_mappings[args.source]
    else:
        raise ValueError("Unsupported source. Please specify 'amazon' or 'lazada'.")

    # Consume Kafka messages and append to HDFS
    consume_kafka_to_hdfs(paths['kafka_topic'], paths['train_file_path'])
    consume_kafka_to_hdfs(paths['kafka_topic'], paths['test_file_path'])

    print(f"Kafka messages consumed and appended to HDFS for {args.source} data.")
