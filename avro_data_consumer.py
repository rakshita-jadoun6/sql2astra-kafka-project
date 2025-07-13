import os
import datetime
import threading
import pyodbc
import json
import argparse
import json


from decimal import *
from time import sleep
from uuid import uuid4, UUID
from dotenv import load_dotenv
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from datetime import datetime


load_dotenv()
# Parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--file_path', type=str, required=True, help='Path to the JSON file')
parser.add_argument('--group_id', type=str, default='group1', help='Kafka consumer group ID')  # <-- Add this line
args = parser.parse_args()
    

# Define Kafka Configuration
kafka_config={
    'bootstrap.servers':os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    'group.id': args.group_id,
    'auto.offset.reset': 'latest'
}

# Define Schema Registery Client
schema_registry_client = SchemaRegistryClient({
    'url': os.getenv('SCHEMA_REGISTRY_URL'),
    'basic.auth.user.info': '{}:{}'.format(os.getenv('SCHEMA_REGISTRY_API_KEY'), os.getenv('SCHEMA_REGISTRY_API_SECRET'))
})

# Fetch the latest Avro schema for the value
subject_name = os.getenv('SUBJECT_NAME')
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf-8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,  # Key will be deserialized as a string
    'value.deserializer': avro_deserializer,  # Value will be deserialized as Avro 
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset'],
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 5000     #auto commit every 5 seconds
})


# To handle serialization of datetime objects, defining a customer encoder.
def datetime_encoder(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    
# Path to the separate JSON file for each consumer
file_path = os.path.join(args.group_id, args.file_path)
# Create the directory if it doesn't exist
os.makedirs(os.path.dirname(file_path), exist_ok=True)

# Python function to append the json string data into json file
# 'a' appends to the file without deleting existing content.
def write_to_json_file_append_mode(json_string, file_path):
    with open(file_path, 'a') as file:
        file.write(json_string+'\n')
    
# Python function to write the json string data into json file
# 'w' creates the file if it doesn't exist and overwrites the file completely.
def write_to_json_file_write_mode(json_string, file_path):
    with open(file_path, 'w') as file:
        file.write(json_string+'\n')

# Subscribe to the topic
consumer.subscribe([os.getenv('TOPIC')])

# Continually read messages from Kafka
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue
        
        # Change the category column to lowercase
        msg.value()['category']=msg.value()['category'].lower()

        # Updating the price to half if product belongs to 'category a'
        if msg.value()['category'] == 'category a':
            
            msg.value()['price'] = msg.value()['price'] * 0.5
            msg.value()['price'] = round(msg.value()['price'],2)

        print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))
        json_string = json.dumps(msg.value(), default=datetime_encoder)

        if not os.path.isfile(file_path):
            write_to_json_file_write_mode(json_string, file_path)
        else:
            write_to_json_file_append_mode(json_string, file_path)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()






