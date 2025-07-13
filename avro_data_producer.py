import os
import datetime
import threading
import pyodbc
import json

from decimal import *
from time import sleep
from uuid import uuid4, UUID
from dotenv import load_dotenv
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from datetime import datetime


load_dotenv()


def delivery_report(err,msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print('Delivery failed for User record {}:{}'.format(msg.key(),err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(msg.key(), msg.topic(), msg.partition(), msg.offset()))


# Define Database Connection
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('SQL_SERVER')};"
    f"DATABASE={os.getenv('SQL_DATABASE')};"
    f"UID={os.getenv('SQL_USERNAME')};"
    f"PWD={os.getenv('SQL_PASSWORD')}"
)
cursor = conn.cursor()

# Define Kafka Configuration
kafka_config={
    'bootstrap.servers':os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD')
}

# Define Schema Registery Client
schema_registry_client = SchemaRegistryClient({
    'url': os.getenv('SCHEMA_REGISTRY_URL'),
    'basic.auth.user.info': '{}:{}'.format(os.getenv('SCHEMA_REGISTRY_API_KEY'), os.getenv('SCHEMA_REGISTRY_API_SECRET'))
})

# Fetch the latest Avro schema for the value
subject_name = os.getenv('SUBJECT_NAME')
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Serializer for the value
key_serializer = StringSerializer('utf-8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro  
})

# Load the last read timestamp from the config file
config_data = {}
default_timestamp = '1900-01-01 00:00:00.000000'
last_read_timestamp = None
second_last_read_timestamp = None

try:
    with open('config.json') as f:
        config_data = json.load(f)
        last_read_timestamp = config_data.get('last_read_timestamp')
        second_last_read_timestamp = config_data.get('second_last_read_timestamp')
except FileNotFoundError:
    pass


# Set a default value for last_read_timestamp
if last_read_timestamp is None:
    last_read_timestamp = default_timestamp
    second_last_read_timestamp = default_timestamp

# Use the last_read_timestamp in the SQL query
query = "SELECT * FROM {} where last_updated > '{}'".format(os.getenv('TABLE'), last_read_timestamp)
cursor.execute(query)

# Check if there are any rows fetched
rows = cursor.fetchall()
if not rows:
    print('No rows to fetch.')
else:
    # Iterate over the cursor and produce to Kafka
    for row in rows:
        # Get the column names from the cursor description
        columns = [column[0] for column in cursor.description]
        # Create a dictionary from the row values
        value = dict(zip(columns, row))
        # Produce to Kafka
        producer.produce(topic=os.getenv('TOPIC'), key=str(value['ID']), value=value, on_delivery=delivery_report)
        producer.flush()

# Fetch any remaining rows to consume the result
cursor.fetchall()

query = "SELECT MAX(last_updated) FROM {}".format(os.getenv('TABLE'))
cursor.execute(query)

# Fetch the result
result = cursor.fetchone()
max_date = result[0]  # Assuming the result is a single value

# Convert datetime object to string representation
if max_date is None:
    print("No rows to fetch.")
    config_data['second_last_read_timestamp'] = default_timestamp
    config_data['last_read_timestamp'] = default_timestamp
else:
    max_date_str = max_date.strftime("%Y-%m-%d %H:%M:%S.%f")
    config_data['second_last_read_timestamp'] = last_read_timestamp
    config_data['last_read_timestamp'] = max_date_str

with open('config.json', 'w') as file:
    json.dump(config_data, file)

# Close the cursor and database connection
cursor.close()
conn.close()

print("Data successfully published to Kafka.")