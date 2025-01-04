import json
import os
import sys
import argparse
import logging
from datetime import datetime, timezone
from time import sleep
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from uuid_utils import UUIDUtility
import csv
import pandas as pd
from typing import List, Optional, Dict
import traceback
import xml.etree.ElementTree as ET
import yaml
from bestprof_utils import parse_pfd, parse_bestprof

file_type_lookup_table = None

# Function to read CSV data globally
def create_file_type_lookup_table(file_path):
    global file_type_lookup_table
    file_type_lookup_table = pd.read_csv(file_path, header=0)
    return file_type_lookup_table


def calculate_spin(f=None, fdot=None, p=None, pdot=None):
        # calculate p and pdot from f and fdot
        if f is not None and fdot is not None:
            p = 1 / f
            pdot = -fdot / (f**2)
        # calculate f and fdot from p and pdot
        elif p is not None and pdot is not None:
            f = 1 / p
            fdot = -pdot * (p**2)
        else:
            raise ValueError("Either (f, fdot) or (p, pdot) must be provided")
            
        return f, fdot, p, pdot

def generate_file_hash(filepath: str) -> str:
    """
    Generates a SHA-256 hash for the contents of the given file.
    
    Parameters:
        filepath (str): The path to the file for which to generate the hash.
        
    Returns:
        str: The SHA-256 hash of the file contents.
    """
    import hashlib
    hash_sha256 = hashlib.sha256()
    
    try:
        with open(filepath, 'rb') as file:
            for chunk in iter(lambda: file.read(4096), b""):
                hash_sha256.update(chunk)
    except FileNotFoundError:
        raise Exception(f"File not found: {filepath}")
    except IOError:
        raise Exception(f"Error reading file: {filepath}")
    
    return hash_sha256.hexdigest()

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
                'top_level': ['session_id', 'run_name', 'task_id', 'status', 'attempt', 'submit', 'workdir', 'task_name'],
                'inputs': ['pipeline_id', 'hardware_id', 'beam_id', 'filtool_id', 'peasoup_id', 'pulsarx_id', 'prepfold_id', 'circular_orbit_search_id', 'elliptical_orbit_search_id', 'rfifind_id', 'candidate_filter_id', 'execution_order', 'program_name', 'input_dp', 'input_dp_id', 'process_input_dp_id'],
                'outputs': []
            },
            'RUNNING': {
                'top_level': ['session_id', 'run_name', 'task_id', 'status', 'attempt', 'submit', 'start', 'workdir', 'task_name'],
                'inputs': ['pipeline_id', 'hardware_id', 'beam_id', 'filtool_id', 'peasoup_id', 'pulsarx_id', 'prepfold_id', 'circular_orbit_search_id', 'elliptical_orbit_search_id', 'rfifind_id', 'candidate_filter_id', 'execution_order', 'program_name'],
                'outputs': []
            },
            'COMPLETED': {
                'top_level': ['session_id', 'run_name', 'task_id', 'status', 'attempt', 'submit', 'start', 'complete', 'workdir', 'task_name'],
                'inputs': ['pipeline_id', 'hardware_id', 'beam_id', 'filtool_id', 'peasoup_id', 'pulsarx_id', 'prepfold_id', 'circular_orbit_search_id', 'elliptical_orbit_search_id', 'rfifind_id', 'candidate_filter_id', 'execution_order', 'program_name', 'input_dp', 'input_dp_id', 'process_input_dp_id'],
                'outputs': ['output_dp', 'output_dp_id', 'fft_size', 'tsamp', 'tobs', 'nsamples', 'freq_start_mhz', 'freq_end_mhz', 'tstart', 'tstart_utc', 'nchans', 'nbits', 'foff', 'locked', 'filehash', 'metainfo', 'pulsarx_cands_file', 'fold_candidate_id', 'search_fold_merged', 'coherent_dm', 'subband_dm']
            },
            'FAILED': {
                'top_level': ['session_id', 'run_name', 'task_id', 'status', 'attempt', 'submit', 'start', 'complete', 'workdir', 'task_name'],
                'inputs': ['pipeline_id', 'hardware_id', 'beam_id', 'filtool_id', 'peasoup_id', 'pulsarx_id', 'prepfold_id', 'circular_orbit_search_id', 'elliptical_orbit_search_id', 'rfifind_id', 'candidate_filter_id', 'execution_order', 'program_name'],
                'outputs': []
            },
            'ABORTED': {
                'top_level': ['session_id', 'run_name', 'task_id', 'status', 'attempt', 'submit', 'start', 'complete', 'workdir', 'task_name'],
                'inputs': ['pipeline_id', 'hardware_id', 'beam_id', 'filtool_id', 'peasoup_id', 'pulsarx_id', 'prepfold_id', 'circular_orbit_search_id', 'elliptical_orbit_search_id', 'rfifind_id', 'candidate_filter_id', 'execution_order', 'program_name'],
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
        int_fields = ['task_id', 'pipeline_id', 'hardware_id', 'filtool_id', 'execution_order', 'attempt_number', 'max_attempts', 'peasoup_id', 'prepfold_id', 'pulsarx_id', 'circular_orbit_search_id', 'elliptical_orbit_search_id', 'rfifind_id', 'candidate_filter_id', 'fft_size']
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
                    row[field] = UUIDUtility.convert_uuid_string_to_binary(row[field])
                except ValueError as e:
                    raise ValueError(f"Error converting {field} to binary. Invalid UUID: {row[field]}. Error: {str(e)}")

        return row


class ProcessingHandler:

    datetime_keys = ['submit_time', 'start_time', 'end_time']
    column_order = [
        'session_id', 'task_id', 'run_name', 'pipeline_id', 'hardware_id', 'submit_time', 'start_time', 'end_time',
        'process_status', 'attempt_number', 'max_attempts', 'workdir', 'peasoup_id', 'pulsarx_id',
        'prepfold_id', 'filtool_id', 'circular_orbit_search_id', 'elliptical_orbit_search_id',
        'rfifind_id', 'candidate_filter_id', 'execution_order', 'program_name'
    ]
    rename_columns = {
        'submit': 'submit_time',
        'start': 'start_time',
        'complete': 'end_time',
        'attempt': 'attempt_number',
        'status': 'process_status',
    }
    columns_to_ignore = {'input_dp_id', 'input_dp'}

    @staticmethod
    def filter_and_rename(data):
        # Helper function to rename, filter, and format a single dictionary
        def rename_and_filter(item):
            renamed_item = {
                ProcessingHandler.rename_columns.get(k, k): v
                for k, v in item.items() if k not in ProcessingHandler.columns_to_ignore
            }
            return renamed_item

        # Process the input (single dictionary or list of dictionaries)
        if isinstance(data, dict):
            filtered_data = [rename_and_filter(data)]
        elif isinstance(data, list):
            filtered_data = [rename_and_filter(item) for item in data]
        else:
            raise TypeError("Input should be a dictionary or a list of dictionaries")

        # Determine columns to use based on column_order
        columns_to_use = [
            col for col in ProcessingHandler.column_order
            if col in set().union(*(d.keys() for d in filtered_data))
        ]

        # Fill missing keys and format datetime fields
        ProcessingHandler.fill_missing_keys(filtered_data, columns_to_use)
        ProcessingHandler.format_datetime(filtered_data, ProcessingHandler.datetime_keys)

        # Return the filtered data, flattening if the original input was a single dictionary
        return filtered_data if isinstance(data, list) else filtered_data[0]


    @staticmethod
    def fill_missing_keys(data, columns_to_use):
        for item in data:
            for key in columns_to_use:
                if key not in item:
                    item[key] = None

    @staticmethod
    def format_datetime(data, datetime_keys):
        for item in data:
            for key in datetime_keys:
                if key in item and item[key]:
                    dt = datetime.fromisoformat(item[key].rstrip('Z'))
                    dt = dt.replace(tzinfo=timezone.utc)
                    item[key] = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


class DataProductInputHandler:
    def __init__(self, kafka_producer, dp_topic):
        self.kafka_producer = kafka_producer
        self.dp_topic = dp_topic

    
    column_order = ['dp_id', 'session_id', 'task_id']
    rename_columns = {
        'input_dp_id': 'dp_id',
    }

    def fill_missing_keys(data, fields):
        for entry in data:
            for field in fields:
                if field not in entry:
                    entry[field] = None

    @staticmethod
    def filter_and_rename(data):
        # Helper function to rename and filter a single dictionary
        def rename_and_filter(item):
            renamed_item = {DataProductInputHandler.rename_columns.get(k, k): v for k, v in item.items()}
            columns_to_use = [
                col for col in DataProductInputHandler.column_order
                if col in renamed_item
            ]
            filtered_item = {k: v for k, v in renamed_item.items() if k in columns_to_use}
            return filtered_item

        # Check if the input is a single dictionary or a list of dictionaries
        if isinstance(data, dict):
            return rename_and_filter(data)
        elif isinstance(data, list):
            return [rename_and_filter(item) for item in data]
        else:
            raise TypeError("Input should be a dictionary or a list of dictionaries")


    def send_data_product_inputs(self, session_id, task_id, input_dp_id):
        data_product_ids = input_dp_id.split()
        for dp_id in data_product_ids:
            
            dp_uuid = UUIDUtility.convert_uuid_string_to_binary(dp_id)
            message = {
                'dp_id': dp_uuid,
                'session_id': session_id,
                'task_id': task_id
            }
            
            self.kafka_producer.produce_message(self.dp_topic, message)


class DataProductOutputHandler:

    def __init__(self, kafka_producer_dp_output, dp_output_topic: str, kafka_producer_search_candidate, search_cand_topic: str, kafka_producer_fold_candidate, fold_cand_topic: str):
        self.kafka_producer_dp_output = kafka_producer_dp_output
        self.dp_output_topic = dp_output_topic
        self.kafka_producer_search_candidate = kafka_producer_search_candidate
        self.search_cand_topic = search_cand_topic
        self.kafka_producer_fold_candidate = kafka_producer_fold_candidate
        self.fold_cand_topic = fold_cand_topic

    column_order = ['id', 'beam_id', 'file_type_id', 'filename', \
                    'filepath', 'filehash', 'available', 'metainfo', \
                    'locked', 'utc_start', 'tsamp', 'tobs', 'nsamples', \
                    'freq_start_mhz', 'freq_end_mhz',   \
                    'hardware_id', 'mjd_start', 'fft_size', 'nchans', 'nbits', 'coherent_dm', 'subband_dm', 'session_id', 'task_id', 'process_status', 'workdir', 'pulsarx_cands_file', 'input_dp', 'input_dp_id', 'fold_candidate_id', 'search_fold_merged']
    
    rename_columns = {
        'output_dp_id': 'id',
        'output_dp': 'filename',
        'tstart': 'mjd_start',
        'tstart_utc': 'utc_start',
        'status': 'process_status',
        'session_id': 'created_by_session_id',
        'task_id': 'created_by_task_id'
    }
  

    def fill_missing_keys(data, fields):
        for entry in data:
            for field in fields:
                if field not in entry:
                    entry[field] = None
    

    @staticmethod
    def filter_and_rename(data):
        # Helper function to rename and filter a single dictionary
        def rename_and_filter(item):
            renamed_item = {
                DataProductOutputHandler.rename_columns.get(k, k): v
                for k, v in item.items()
            }
            return renamed_item

        # Process the input (single dictionary or list of dictionaries)
        if isinstance(data, dict):
            filtered_data = [rename_and_filter(data)]
        elif isinstance(data, list):
            filtered_data = [rename_and_filter(item) for item in data]
        else:
            raise TypeError("Input should be a dictionary or a list of dictionaries")

        # Determine columns to use based on column_order
        columns_to_use = [
            col for col in DataProductOutputHandler.column_order
            if col in set().union(*(d.keys() for d in filtered_data))
        ]

        # Filter each item to only include the columns in columns_to_use
        filtered_data = [
            {k: v for k, v in item.items() if k in columns_to_use}
            for item in filtered_data
        ]

        # Return the filtered data, flattening if the original input was a single dictionary
        return filtered_data if isinstance(data, list) else filtered_data[0]
    
    @staticmethod
    def get_xml_cands(xml_file):

        tree = ET.parse(xml_file)
        root = tree.getroot()
        header_params = root[1]
        search_params = root[2]
        segment_params = root[3]
        candidates = root[7]

        segment_start_sample = int(segment_params.find('segment_start_sample').text)
        segment_nsamples = int(segment_params.find('segment_nsamples').text)
        segment_pepoch = float(segment_params.find('segment_pepoch').text)

        
        ignored_entries = ['candidate', 'opt_period', 'folded_snr', 'byte_offset', 'is_adjacent', 'is_physical']
        rows = []
        for candidate in candidates:
            cand_dict = {}
            for cand_entry in candidate.iter():
                if not cand_entry.tag in ignored_entries:
                    cand_dict[cand_entry.tag] = cand_entry.text
            cand_dict['cand_id_in_file'] = candidate.attrib.get("id")
            cand_dict['segment_start_sample'] = segment_start_sample
            cand_dict['segment_nsamples'] = segment_nsamples
            cand_dict['segment_pepoch'] = segment_pepoch
            rows.append(cand_dict)

        df = pd.DataFrame(rows)
        df = df.astype({"snr": float, "dm": float, "period": float, "nh": int, "acc": float, "nassoc": int, "ddm_count_ratio": float, "ddm_snr_ratio": float,  "cand_id_in_file": int, "segment_start_sample": int, "segment_nsamples": int, "segment_pepoch": float})

        return df

    
    def xml_to_kafka_producer(self, xml_file, beam_id, hardware_id, dp_id):

        df = self.get_xml_cands(xml_file)
    
        for index, row in df.iterrows():
            message = {}
            message['id'] = UUIDUtility.convert_uuid_string_to_binary(row['search_candidates_database_uuid'])
            message['spin_period'] = row['period']
            message['dm'] = row['dm']
            message['pdot'] = a_to_pdot(row['period'], row['acc'])
            message['snr'] = row['snr']
            message['ddm_count_ratio'] = row['ddm_count_ratio']
            message['ddm_snr_ratio'] = row['ddm_snr_ratio']
            message['nassoc'] = int(row['nassoc'])
            message['filename'] = os.path.basename(xml_file)
            message['filepath'] = os.path.dirname(xml_file)
            message['nh'] = int(row['nh'])
            message['dp_id'] = dp_id
            message['candidate_id_in_file'] = int(row['cand_id_in_file'])
            message['segment_start_sample'] = int(row['segment_start_sample'])
            message['segment_nsamples'] = int(row['segment_nsamples'])
            message['segment_pepoch'] = float(row['segment_pepoch'])
            
            self.kafka_producer_search_candidate.produce_message(self.search_cand_topic, message)
    
    def pulsarx_to_kafka_producer(self, search_fold_merged_file):
        
        results = pd.read_csv(search_fold_merged_file)
        if results.empty:
            logging.error(f"No output found in {search_fold_merged_file}")

        for index, row in results.iterrows():
            message = {}
            message['id'] = UUIDUtility.convert_uuid_string_to_binary(row['fold_candidates_database_uuid'])
            f, fdot, p, pdot = calculate_spin(f=row['f0_new'], fdot=row['f1_new'])
            message['spin_period'] = p
            message['dm'] = row['dm_new']
            message['pdot'] = pdot
            message['fold_snr'] = row['S/N_new']
            message['search_candidate_id'] = UUIDUtility.convert_uuid_string_to_binary(row['search_candidates_database_uuid'])
            message['dp_id'] = UUIDUtility.convert_uuid_string_to_binary(row['fold_dp_output_uuid'])
            
            #Send to kafka
            self.kafka_producer_fold_candidate.produce_message(self.fold_cand_topic, message)

    def prepfold_to_kafka_producer(self, search_fold_merged_file):

        results = pd.read_csv(search_fold_merged_file)
      
        if results.empty:
            logging.error(f"No output found in {search_fold_merged_file}")
        
       
        for index, row in results.iterrows():

            message = {}
            message['id'] = UUIDUtility.convert_uuid_string_to_binary(row['fold_candidates_database_uuid'])
            message['spin_period'] = row['p0_new']
            message['dm'] = row['dm_new']
            message['pdot'] = row['p1_new']
            message['fold_snr'] = row['S/N_new']
            message['search_candidate_id'] = UUIDUtility.convert_uuid_string_to_binary(row['search_candidates_database_uuid'])
            message['dp_id'] = UUIDUtility.convert_uuid_string_to_binary(row['fold_dp_output_uuid'])
           
            #Send to kafka
            self.kafka_producer_fold_candidate.produce_message(self.fold_cand_topic, message)

            
            

    def send_data_product_outputs(
        self,
        session_id: str,
        task_id: int,
        taskname: str,
        workdir: str,
        beam_id: int,
        input_dp: str,
        filename_list: str,
        output_dp_id_list: str,
        hardware_id: int,
        file_type_table: pd.DataFrame,
        generate_file_hash=False,
        **optional_fields
    ):
        data_product_ids = output_dp_id_list.split()
        filenames = filename_list.split()

        for dp_id, filename in zip(data_product_ids, filenames):
            
            dp_uuid = UUIDUtility.convert_uuid_string_to_binary(dp_id)
            filepath = workdir
            basename = os.path.basename(filename)
            file_extension = os.path.splitext(filename)[1].lstrip('.')
            file_type_id = int(file_type_table.loc[file_type_table['name'] == file_extension, 'id'].values[0])
           

            if generate_file_hash:
                optional_fields['filehash'] = generate_file_hash(filename)
            
           
            message = {
                'id': dp_uuid,
                'beam_id': beam_id,
                'file_type_id': file_type_id,
                'filename': basename,
                'filepath': filepath,
                'created_by_session_id': session_id,
                'created_by_task_id': task_id,
                'hardware_id': hardware_id,
                'available': optional_fields.get('available', 1),
                'tsamp': optional_fields.get('tsamp'),
                'tobs': optional_fields.get('tobs'),
                'nsamples': optional_fields.get('nsamples'),
                'freq_start_mhz': optional_fields.get('freq_start_mhz'),
                'freq_end_mhz': optional_fields.get('freq_end_mhz'),
                'locked': optional_fields.get('locked'),
                'filehash': optional_fields.get('filehash'),
                'metainfo': optional_fields.get('metainfo'),
                'utc_start': optional_fields.get('utc_start'),
                'mjd_start': optional_fields.get('mjd_start'),
                'fft_size': optional_fields.get('fft_size'),
                'nchans': optional_fields.get('nchans'),
                'nbits': optional_fields.get('nbits'),
                'coherent_dm': optional_fields.get('coherent_dm'),
                'subband_dm': optional_fields.get('subband_dm')
            }

            for key in ('fft_size', 'nsamples', 'nchans', 'nbits'):
                if message.get(key) is not None:
                    try:
                        message[key] = int(message[key])
                    except ValueError:
                        logging.error(f"Invalid value for {key}: {message[key]}")
                        message[key] = None

            for key in ('tsamp', 'tobs', 'freq_start_mhz', 'freq_end_mhz', 'coherent_dm', 'subband_dm'):
                if message.get(key) is not None:
                    try:
                        #These are passed as strings because the DB has decimal data type.
                        message[key] = str(message[key])
                    except ValueError:
                        logging.error(f"Invalid value for {key}: {message[key]}")
                        message[key] = None
            message = {k: v for k, v in message.items() if v is not None}
            
            
            self.kafka_producer_dp_output.produce_message(self.dp_output_topic, message)
            #If its an xml file, then we need to send the search candidate to the search candidate topic
            if file_extension == 'xml':
                self.xml_to_kafka_producer(filename, beam_id, hardware_id, dp_uuid)
        
        #PulsarX folds
        if taskname.startswith("pulsarx"):
            search_fold_merged_file = f"{workdir}/{optional_fields.get('search_fold_merged')}"
            self.pulsarx_to_kafka_producer(search_fold_merged_file)
        if taskname.startswith("prepfold"):
            search_fold_merged_file = f"{workdir}/{optional_fields.get('search_fold_merged')}"
            self.prepfold_to_kafka_producer(search_fold_merged_file)

class JsonFileProcessor(FileSystemEventHandler):
    def __init__(self, directory, kafka_producer_processing, kafka_producer_dp_input, kafka_producer_dp_output, kafka_producer_search_candidate, kafka_producer_fold_candidate, processing_topic, dp_input_topic, dp_output_topic, search_cand_topic, fold_cand_topic, read_existing=False):
        super().__init__()
        self.directory = directory
        self.kafka_producer_processing = kafka_producer_processing
        self.kafka_producer_dp_input = kafka_producer_dp_input
        self.kafka_producer_dp_output = kafka_producer_dp_output
        self.kafka_producer_search_candidate = kafka_producer_search_candidate
        self.kafka_producer_fold_candidate = kafka_producer_fold_candidate
        self.processing_topic = processing_topic
        self.dp_input_topic = dp_input_topic
        self.dp_output_topic = dp_output_topic
        self.search_cand_topic = search_cand_topic
        self.fold_cand_topic = fold_cand_topic
        self.read_existing = read_existing
        self.dp_inputs_handler = DataProductInputHandler(kafka_producer_dp_input, dp_input_topic)
        self.dp_outputs_handler = DataProductOutputHandler(kafka_producer_dp_output, dp_output_topic, kafka_producer_search_candidate, search_cand_topic, kafka_producer_fold_candidate, fold_cand_topic)

    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith('.json'):
            return
        logging.info(f"New file detected: {event.src_path}")
        sleep(4)  # Ensure the file is fully written
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
            logging.error(traceback.format_exc())
    
    def process_data(self, data):
        
        for item in data:
          
            filtered_data_processing = ProcessingHandler.filter_and_rename(item)
            filtered_data_processing = KafkaProducer.convert_row_types(filtered_data_processing) 
            self.kafka_producer_processing.produce_message(self.processing_topic, filtered_data_processing)

            if item['status'] == 'SUBMITTED':
                filtered_data_processing_dp_inputs = DataProductInputHandler.filter_and_rename(item)
                session_id = filtered_data_processing_dp_inputs.get('session_id')
                task_id = filtered_data_processing_dp_inputs.get('task_id')
                input_dp_id = filtered_data_processing_dp_inputs.get('dp_id')
                if input_dp_id:
                   self.dp_inputs_handler.send_data_product_inputs(session_id, task_id, input_dp_id)
            
            if item['status'] == 'COMPLETED':
                filtered_data_dp_outputs = DataProductOutputHandler.filter_and_rename(item)                
                self.dp_outputs_handler.send_data_product_outputs(
                    session_id = filtered_data_processing['session_id'],
                    task_id = filtered_data_processing['task_id'],
                    taskname = filtered_data_processing['task_name'],
                    workdir = filtered_data_dp_outputs['workdir'],
                    beam_id = filtered_data_dp_outputs['beam_id'],
                    input_dp = filtered_data_dp_outputs['input_dp'],
                    filename_list = filtered_data_dp_outputs['filename'],
                    output_dp_id_list = filtered_data_dp_outputs['id'],
                    hardware_id = filtered_data_dp_outputs['hardware_id'],
                    file_type_table = file_type_lookup_table,
                    filehash = filtered_data_dp_outputs.get('filehash'),
                    tsamp = filtered_data_dp_outputs.get('tsamp'),
                    tobs = filtered_data_dp_outputs.get('tobs'),
                    nsamples = filtered_data_dp_outputs.get('nsamples'),
                    freq_start_mhz = filtered_data_dp_outputs.get('freq_start_mhz'),
                    freq_end_mhz = filtered_data_dp_outputs.get('freq_end_mhz'),
                    locked = filtered_data_dp_outputs.get('locked'),
                    metainfo = filtered_data_dp_outputs.get('metainfo'),
                    utc_start = filtered_data_dp_outputs.get('utc_start'),
                    mjd_start = filtered_data_dp_outputs.get('mjd_start'),
                    fft_size = filtered_data_dp_outputs.get('fft_size'),
                    nchans = filtered_data_dp_outputs.get('nchans'),
                    nbits = filtered_data_dp_outputs.get('nbits'),
                    coherent_dm = filtered_data_dp_outputs.get('coherent_dm'),
                    subband_dm = filtered_data_dp_outputs.get('subband_dm'),
                    search_fold_merged = filtered_data_dp_outputs.get('search_fold_merged')
                )



def a_to_pdot(P_s, acc_ms2):
    LIGHT_SPEED = 2.99792458e8  # Speed of Light in SI
    return P_s * acc_ms2 /LIGHT_SPEED



def load_config(config_file):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

def main(config):
    # Determine the directory to monitor
    if config['directory'] is None:
        # Get the directory of the current script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        directory = os.path.join(script_dir, 'results/')
    else:
        directory = config['directory']

    # Use config values as needed
    bootstrap_servers = config['bootstrap_servers']
    schema_registry_url = config['schema_registry_url']
    processing_topic = config['processing_topic']
    processing_schema_file = config['processing_schema_file']
    dp_input_topic = config['dp_input_topic']
    dp_input_schema_file = config['dp_input_schema_file']
    dp_output_topic = config['dp_output_topic']
    dp_output_schema_file = config['dp_output_schema_file']
    search_cand_topic = config['search_cand_topic']
    search_cand_schema_file = config['search_cand_schema_file']
    fold_cand_topic = config['fold_cand_topic']
    fold_cand_schema_file = config['fold_cand_schema_file']
    read_existing = config['read_existing']

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    kafka_producer_processing = KafkaProducer(bootstrap_servers, schema_registry_url, processing_schema_file)
    kafka_producer_dp_input = KafkaProducer(bootstrap_servers, schema_registry_url, dp_input_schema_file)
    kafka_producer_dp_output = KafkaProducer(bootstrap_servers, schema_registry_url, dp_output_schema_file)
    kafka_producer_search_candidate = KafkaProducer(bootstrap_servers, schema_registry_url, search_cand_schema_file)
    kafka_producer_fold_candidate = KafkaProducer(bootstrap_servers, schema_registry_url, fold_cand_schema_file)

    event_handler = JsonFileProcessor(
        directory=directory,
        kafka_producer_processing=kafka_producer_processing,
        kafka_producer_dp_input=kafka_producer_dp_input,
        kafka_producer_dp_output=kafka_producer_dp_output,
        kafka_producer_search_candidate=kafka_producer_search_candidate,
        kafka_producer_fold_candidate=kafka_producer_fold_candidate,
        processing_topic=processing_topic,
        dp_input_topic=dp_input_topic,
        dp_output_topic=dp_output_topic,
        search_cand_topic=search_cand_topic,
        fold_cand_topic=fold_cand_topic,
        read_existing=read_existing
    )

    create_file_type_lookup_table("file_type.csv")

    if read_existing:
        for filename in sorted(os.listdir(directory)):
            if filename.endswith('.json'):
                file_path = os.path.join(directory, filename)
                event_handler.process_json(file_path)
                sleep(2)

    #observer = Observer()
    observer = PollingObserver()
    observer.schedule(event_handler, directory, recursive=False)
    observer.start()

    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    config = load_config('watchdog.yaml')
    main(config)

