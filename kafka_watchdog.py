import json
import os
import sys
import argparse
import logging
from datetime import datetime, timezone
from time import sleep
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import uuid_utils

datetime_keys = ['submit_time', 'start_time', 'end_time']

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
                dt = datetime.fromisoformat(item[key].rstrip('Z'))
                dt = dt.replace(tzinfo=timezone.utc)
                item[key] = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

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
                'inputs': ['process_uuid', 'pipeline_id', 'hardware_id', 'filtool_id', 'peasoup_id', 'pulsarx_id', 'prepfold_id', 'circular_orbit_search_id', 'elliptical_orbit_search_id', 'rfifind_id', 'candidate_filter_id', 'execution_order', 'program_name', 'input_dp_id'],
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
                'outputs': ['output_filename', 'output_dir', 'output_file_type_id', 'output_file_uuid', 'fft_size', 'tsamp', 'tobs', 'nsamples', 'freq_start_mhz', 'freq_end_mhz', 'tstart', 'tstart_utc', 'nchans', 'nbits']
            },
            'FAILED': {
                'top_level': ['status', 'attempt', 'submit', 'start', 'complete'],
                'inputs': ['process_uuid', 'pipeline_id', 'hardware_id', 'filtool_id', 'peasoup_id', 'pulsarx_id', 'prepfold_id', 'circular_orbit_search_id', 'elliptical_orbit_search_id', 'rfifind_id', 'candidate_filter_id', 'execution_order', 'program_name'],
                'outputs': []
            },
            'ABORTED': {
                'top_level': ['status', 'attempt', 'submit', 'start', 'complete'],
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
        
        for field in status_fields['top_level']:
            extracted[field] = event.get(field, None)
        
        for input_field in status_fields['inputs']:
            for item in event['task']['inputs']:
                if item['name'] == input_field:
                    extracted[input_field] = item.get('value', None)

        for output_field in status_fields['outputs']:
            for item in event['task']['outputs']:
                if item['name'] == output_field:
                    extracted[output_field] = item.get('value', None)

        return extracted

class KafkaProducer:
    def __init__(self, bootstrap_servers, schema_registry_url, schema_file_path):
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.schema_file_path = schema_file_path
        self.schema_str = self._load_schema()
        self.producer = self._init_producer()
        self.value_serializer = self._init_serializer()

    def _load_schema(self):
        with open(self.schema_file_path, "r") as schema_file:
            return schema_file.read()

    def _init_producer(self):
        producer_conf = {
            'bootstrap.servers': self.bootstrap_servers,
        }
        return Producer(producer_conf)

    def _init_serializer(self):
        schema_registry_client = SchemaRegistryClient({"url": self.schema_registry_url})
        return AvroSerializer(schema_str=self.schema_str, schema_registry_client=schema_registry_client)

    def produce_message(self, topic, value):
        value_bytes = self.value_serializer(value, SerializationContext(topic, MessageField.VALUE))
        self.producer.produce(topic=topic, value=value_bytes, on_delivery=self.delivery_report)
        self.producer.flush()

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            logging.error(f"Delivery failed for User record {msg.key()}: {err}")
            return
        logging.info(f'User record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

    @staticmethod
    def convert_row_types(row):
        int_fields = ['pipeline_id', 'hardware_id', 'filtool_id', 'execution_order', 'attempt_number', 'max_attempts', 'peasoup_id', 'prepfold_id', 'pulsarx_id', 'circular_orbit_search_id', 'elliptical_orbit_search_id', 'rfifind_id', 'candidate_filter_id']
        binary_fields = ['id']

        for key, value in row.items():
            if value == '':
                row[key] = None

        for field in int_fields:
            if field in row and row[field] is not None:
                try:
                    row[field] = int(row[field])
                except ValueError:
                    raise ValueError(f"Error converting {field} to int. Invalid value: {row[field]}")

        for field in binary_fields:
            if field in row and row[field] is not None:
                try:
                    row[field] = uuid_utils.convert_uuid_string_to_binary(row[field])
                except ValueError as e:
                    raise ValueError(f"Error converting {field} to binary. Invalid UUID: {row[field]}. Error: {str(e)}")

        return row

class ProcessDataProductInputs:
    def __init__(self, kafka_producer, dp_topic):
        self.kafka_producer = kafka_producer
        self.dp_topic = dp_topic

    def send_data_product_inputs(self, processing_id, input_dp_id):
        data_product_ids = input_dp_id.split()

        for dp_id in data_product_ids:
            new_id = uuid_utils.generate_binary_uuid()
            processing_uuid = uuid_utils.convert_uuid_string_to_binary(processing_id)
            dp_uuid = uuid_utils.convert_uuid_string_to_binary(dp_id)
            message = {
                'id': new_id,
                'dp_id': dp_uuid,
                'processing_id': processing_uuid
            }
            
            self.kafka_producer.produce_message(self.dp_topic, message)

class JsonFileHandler(FileSystemEventHandler):
    def __init__(self, directory, kafka_producer_processing, processing_topic, kafka_producer_dp, dp_topic, read_existing=False):
        super().__init__()
        self.directory = directory
        self.kafka_producer_processing = kafka_producer_processing
        self.processing_topic = processing_topic
        self.kafka_producer_dp = kafka_producer_dp
        self.dp_topic = dp_topic
        self.read_existing = read_existing
       
        self.dp_inputs_handler = ProcessDataProductInputs(kafka_producer_dp, dp_topic)

    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith('.json'):
            return
        sleep(2)
        self.process_json(event.src_path)

    def process_json(self, file_path):
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
            parser = ProcessDataParser(data)
            result = parser.parse()
            self.process_data(result)
        except Exception as e:
            logging.error(f"Failed to read or process {file_path}: {e}")

    def process_data(self, data):
        
        # Filter out ignored columns for messages to the processing topic
        filtered_data = [{rename_columns.get(k, k): v for k, v in item.items() if k not in columns_to_ignore} for item in data]
        
        format_datetime(filtered_data, datetime_keys)
        columns_to_use = [col for col in column_order if col in set().union(*(d.keys() for d in filtered_data))]
        fill_missing_keys(filtered_data, columns_to_use)
        format_datetime(filtered_data, datetime_keys)

        for item in filtered_data:
            item = KafkaProducer.convert_row_types(item)
            self.kafka_producer_processing.produce_message(self.processing_topic, item)
        
        # Handle data product input messages when a process is submitted
        for item in data:
            if item['status'] == 'SUBMITTED':
                input_dp_id = item.get('input_dp_id')
                if input_dp_id:
                    self.dp_inputs_handler.send_data_product_inputs(item['process_uuid'], input_dp_id)

def main(args):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    kafka_producer_processing = KafkaProducer(args.bootstrap_servers, args.schema_registry_url, args.schema_file)
    kafka_producer_dp = KafkaProducer(args.bootstrap_servers, args.schema_registry_url, args.dp_schema_file)
    event_handler = JsonFileHandler(args.directory, kafka_producer_processing, args.topic, kafka_producer_dp, args.dp_topic, args.read_existing)
    
    if args.read_existing:
        for filename in sorted(os.listdir(args.directory)):
            if filename.endswith('.json'):
                file_path = os.path.join(args.directory, filename)
                event_handler.process_json(file_path)
                sleep(2)

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
    parser = argparse.ArgumentParser(description="Watch a directory for JSON files and produce messages to Kafka.")
    parser.add_argument("--directory", type=str, help="Directory to monitor for JSON files.", default="/fred/oz005/users/vishnu/one_ring/results")
    parser.add_argument("--topic", type=str, help="Kafka topic to produce the messages to.", required=True)
    parser.add_argument("--schema_registry_url", type=str, help="URL for the schema registry.", required=True)
    parser.add_argument("--schema_file", type=str, help="Schema file for the Avro conversion.", required=True)
    parser.add_argument("--bootstrap_servers", type=str, help="Bootstrap servers for Kafka.", required=True)
    parser.add_argument("--dp_topic", type=str, help="Kafka topic for processing data product inputs.", required=True)
    parser.add_argument("--dp_schema_file", type=str, help="Schema file for the data product inputs Avro conversion.", required=True)
    parser.add_argument("--read_existing", action="store_true", help="Process existing JSON files on startup.")
    args = parser.parse_args()
    
    main(args)
