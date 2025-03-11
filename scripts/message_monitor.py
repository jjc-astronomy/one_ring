import json
import csv
import os
import sys
import argparse
import subprocess
import logging
from datetime import datetime, timezone
from time import sleep
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

datetime_keys = ['submit_time', 'start_time', 'end_time']
#F0 F1(Template-Bank)
#Peasoup In-Built Barcentering! -> Pulsarx -> Ephemeris, accel jerk search with topo and bary.
#CLFD does not clean PRESTO PFDS!
#M30 APSUSE (Filterbank! 25.06 -> 26. 28).
#Filterbank! 64 chan sub-bands. Test Dataset.
 # Desired order of columns
column_order = [
    'id', 'pipeline_id', 'hardware_id', 'submit_time', 'start_time', 'end_time',
    'process_status', 'attempt_number', 'max_attempts', 'peasoup_id', 'pulsarx_id',
    'prepfold_id', 'filtool_id', 'circular_orbit_search_id', 'elliptical_orbit_search_id',
    'rfifind_id', 'candidate_filter_id', 'execution_order', 'program_name'
]

rename_columns = {
        'submit': 'submit_time',
        'start': 'start_time',
        'complete': 'end_time',
        'attempt': 'attempt_number',
        'process_uuid': 'id',
        'status': 'process_status'
    }
columns_to_ignore = {'input_dp_id', 'input_dp'}

def format_datetime(data, datetime_keys):
    for item in data:
        for key in datetime_keys:
            if key in item and item[key]:
                # Parse the datetime assuming it may or may not include 'Z'
                dt = datetime.fromisoformat(item[key].rstrip('Z'))
                # Ensure the datetime is UTC and format to ISO 8601 without 'Z'
                dt = dt.replace(tzinfo=timezone.utc)  # Normalize to UTC
                item[key] = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # MariaDB DATETIME(3) compatible

def fill_missing_keys(data, fields):
    for entry in data:
        for field in fields:
            if field not in entry:
                entry[field] = None  

class ProcessDataParser:
    def __init__(self, json_data):
        self.data = json_data
        self.fields_by_status = {
            'SUBMITTED': {
                'top_level': ['status', 'attempt', 'submit'],
                'inputs': ['process_uuid', 'pipeline_id', 'hardware_id', 'filtool_id', 'peasoup_id', 'pulsarx_id', 'prepfold_id', 'circular_orbit_search_id', 'elliptical_orbit_search_id', 'rfifind_id', 'candidate_filter_id', 'execution_order', 'program_name'],
                'outputs': []
            },
            'RUNNING': {
                'top_level': ['status', 'attempt', 'submit', 'start'],
                'inputs': ['process_uuid', 'pipeline_id', 'hardware_id', 'filtool_id', 'peasoup_id', 'pulsarx_id', 'prepfold_id', 'circular_orbit_search_id', 'elliptical_orbit_search_id', 'rfifind_id', 'candidate_filter_id', 'execution_order', 'program_name'],
                'outputs': []
            },
            'COMPLETED': {
                'top_level': ['status', 'attempt', 'submit', 'start', 'complete'],
                'inputs': ['process_uuid', 'pipeline_id', 'hardware_id', 'filtool_id', 'peasoup_id', 'pulsarx_id', 'prepfold_id', 'circular_orbit_search_id', 'elliptical_orbit_search_id', 'rfifind_id', 'candidate_filter_id', 'execution_order', 'program_name'],
                'outputs': []
            },
            'FAILED': {
                'top_level': ['status', 'attempt', 'complete'],
                'inputs': ['process_uuid', 'pipeline_id', 'hardware_id', 'filtool_id', 'peasoup_id', 'pulsarx_id', 'prepfold_id', 'circular_orbit_search_id', 'elliptical_orbit_search_id', 'rfifind_id', 'candidate_filter_id', 'execution_order', 'program_name'],
                'outputs': []
            }
        }

    def parse(self):
        results = []
        for event in self.data:
            if event['status'] in self.fields_by_status:
                extracted_data = self.extract_data(event)
                results.append(extracted_data)
        return results

    def extract_data(self, event):
        status_fields = self.fields_by_status[event['status']]
        extracted = {'status': event['status']}
        
        # Extracting top-level fields
        for field in status_fields['top_level']:
            extracted[field] = event.get(field, None)
        
        # Extracting input fields
        for input_field in status_fields['inputs']:
            for item in event['task']['inputs']:
                if item['name'] == input_field:
                    extracted[input_field] = item.get('value', None)

        # Extracting output fields
        for output_field in status_fields['outputs']:
            for item in event['task']['outputs']:
                if item['name'] == output_field:
                    extracted[output_field] = item.get('value', None)

        return extracted

class JsonFileHandler(FileSystemEventHandler):
    def __init__(self, directory, topics, schema_registry_url, schema_file, bootstrap_servers, read_existing=False):
        super().__init__()
        self.directory = directory
        self.topics = topics
        self.schema_registry_url = schema_registry_url
        self.schema_file = schema_file
        self.bootstrap_servers = bootstrap_servers
        self.read_existing = read_existing

    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith('.json'):
            return
        sleep(4)
        self.process_json(event.src_path)

    def process_json(self, file_path):
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
            parser = ProcessDataParser(data)
            result = parser.parse()
            self.process_data(result, file_path.replace('.json', '.csv'))
        except Exception as e:
            logging.error(f"Failed to read or process {file_path}: {e}")

    def process_data(self, data, output_file):
        data = [{rename_columns.get(k, k): v for k, v in item.items() if k not in columns_to_ignore} for item in data]
        # Columns and datetime_keys should be globally defined or passed as parameters
        format_datetime(data, datetime_keys) 
        columns_to_use = [col for col in column_order if col in set().union(*(d.keys() for d in data))]
        fill_missing_keys(data, columns_to_use)  
        format_datetime(data, datetime_keys)  
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=columns_to_use)
            writer.writeheader()
            writer.writerows(data)
        self.produce_to_kafka(output_file)

    def produce_to_kafka(self, value_file):
        cmds = f"python kafka_producer.py --bootstrap_servers {self.bootstrap_servers} --schema_registry_url {self.schema_registry_url} --schema_file {self.schema_file} --topic {self.topics} --value_file {value_file}"
        subprocess.run(cmds, shell=True)

def main(args):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    event_handler = JsonFileHandler(args.directory, args.topic, args.schema_registry_url, args.schema_file, args.bootstrap_servers, args.read_existing)
    
    if args.read_existing:
        for filename in sorted(os.listdir(args.directory)):
            if filename.endswith('.json'):
                file_path = os.path.join(args.directory, filename)
                event_handler.process_json(file_path)
                sleep(2)  # Sleep 2 seconds after processing each existing file

    observer = Observer()
    observer.schedule(event_handler, args.directory, recursive=False)
    observer.start()
    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process JSON files and produce to Kafka.")
    parser.add_argument("--directory", type=str, help="Directory to monitor for JSON files.", default="/fred/oz005/users/vishnu/one_ring/results")
    # parser.add_argument("--topic", type=str, required=True, help="Kafka topic to produce the messages to.")
    # parser.add_argument("--schema_registry_url", type=str, required=True, help="URL for the schema registry.")
    # parser.add_argument("--schema_file", type=str, required=True, help="Schema file for the Avro conversion.")
    # parser.add_argument("--bootstrap_servers", type=str, required=True, help="Bootstrap servers for Kafka.")
    parser.add_argument("--read_existing", action="store_true", help="Process existing JSON files on startup.")
    args = parser.parse_args()
    args.topic = "processing"
    args.value_file = "processing_message.csv"
    args.bootstrap_servers = "localhost:9092"
    args.schema_registry_url = "http://localhost:8081"
    args.schema_file = "avro_schema/Processing.avsc"
    
    main(args)
