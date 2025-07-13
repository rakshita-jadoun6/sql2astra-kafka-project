from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime
from dotenv import load_dotenv
import json
import os
import time
import re

load_dotenv()

# Set up cloud configuration using the bundle
cloud_config = {
    'secure_connect_bundle': 'secure-connect-productdb.zip'
}

# Astra credentials
ASTRA_USERNAME = os.getenv('ASTRA_CLIENTID')  
ASTRA_PASSWORD = os.getenv('ASTRA_SECRET')  
ASTRA_KEYSPACE = os.getenv('ASTRA_KEYSPACE')
ASTRA_TABLE = os.getenv('ASTRA_TABLE')

default_timestamp = '1900-01-01 00:00:00.000'

group_folder_pattern = re.compile(r'^group\d+$')

def load_timestamps():
    try:
        with open('config.json') as f:
            config_data = json.load(f)
            last_read_timestamp_str = config_data.get('last_read_timestamp', default_timestamp)
            second_last_read_timestamp_str = config_data.get('second_last_read_timestamp', default_timestamp)
    except FileNotFoundError:
        last_read_timestamp_str = default_timestamp
        second_last_read_timestamp_str = default_timestamp

    last_read = datetime.strptime(last_read_timestamp_str, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=None)
    second_last_read = datetime.strptime(second_last_read_timestamp_str, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=None)
    return last_read, second_last_read

def connect_to_astra():
    auth_provider = PlainTextAuthProvider(ASTRA_USERNAME, ASTRA_PASSWORD)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace(ASTRA_KEYSPACE)

    session.execute(f"""
    CREATE TABLE IF NOT EXISTS {ASTRA_TABLE} (
        record_id     text PRIMARY KEY,
        ID            int,
        name          text,
        category      text,
        price         double,
        last_updated  timestamp
    );
    """)
    return session

def generate_unique_key(session, base_key, last_updated):
    prefix = base_key + "_V"
    max_key_range = prefix + "Z"

    query = f"""
        SELECT record_id, last_updated 
        FROM {ASTRA_TABLE} 
        WHERE record_id >= %s AND record_id < %s ALLOW FILTERING
    """
    rows = session.execute(query, (prefix, max_key_range))

    existing_versions = []
    for row in rows:
        if row.last_updated == last_updated:
            return None  # Duplicate, skip
        match = re.match(rf"^{re.escape(prefix)}(\d+)$", row.record_id)
        if match:
            existing_versions.append(int(match.group(1)))

    next_version = max(existing_versions, default=0) + 1
    return f"{base_key}_V{next_version}"

def process_folders(session, last_read_timestamp, second_last_read_timestamp):
    folders = [name for name in os.listdir('.') if os.path.isdir(name) and group_folder_pattern.match(name)]

    for folder in folders:
        group_prefix = folder.replace("group", "G")

        for filename in os.listdir(folder):
            if filename.endswith('.json'):
                customer_prefix = os.path.splitext(filename)[0].replace('consumer', 'C')
                file_path = os.path.join(folder, filename)

                with open(file_path, 'r') as f:
                    for line_number, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue

                        try:
                            record = json.loads(line)
                            last_updated = datetime.fromisoformat(record['last_updated']).replace(tzinfo=None)

                            if second_last_read_timestamp < last_updated <= last_read_timestamp:
                                base_key = f"{group_prefix}{customer_prefix}_{record['ID']}"
                                final_key = generate_unique_key(session, base_key, last_updated)

                                if final_key:
                                    session.execute(f"""
                                        INSERT INTO {ASTRA_TABLE} (record_id, ID, name, category, price, last_updated)
                                        VALUES (%s, %s, %s, %s, %s, %s)
                                    """, (
                                        final_key,
                                        record['ID'],
                                        record['name'],
                                        record['category'],
                                        float(record['price']),
                                        last_updated
                                    ))
                        except Exception as e:
                            print(f"Error in {file_path}, line {line_number}: {e}")

def main():
    session = connect_to_astra()

    try:
        while True:
            last_read_timestamp, second_last_read_timestamp = load_timestamps()
            process_folders(session, last_read_timestamp, second_last_read_timestamp)
            print("Cycle complete. Sleeping for 10 seconds...")
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nStopped by user. Exiting gracefully.")
    finally:
        session.shutdown()

if __name__ == '__main__':
    main()
