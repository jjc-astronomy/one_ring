import json, os, sys, glob, csv
import subprocess
from datetime import datetime, timezone
from time import sleep





class ProcessDataParser:
    def __init__(self, json_data):
        self.data = json_data
        self.fields_by_status = {
            'SUBMITTED': {
                'top_level': ['status', 'attempt', 'submit'],
                'inputs': ['process_uuid', 'pipeline_id', 'hardware_id', 'filtool_id', 'peasoup_id', 'pulsarx_id', 'prepfold_id', 'circular_orbit_search_id', 'elliptical_orbit_search_id', 'rfifind_id', 'candidate_filter_id', 'execution_order', 'program_name', 'input_dp_id', 'input_dp'],
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
                #'outputs': ['output_filename', 'tsamp', 'tobs', 'nsamples', 'freq_start_mhz', 'freq_end_mhz', 'tstart', 'tstart_utc', 'fft_size', 'nchans', 'nbits']
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

filenames = sorted(glob.glob('results/*.json'))

def fill_missing_keys(data, fields):
    for entry in data:
        for field in fields:
            if field not in entry:
                entry[field] = None  # Use None 


def format_datetime(data, datetime_keys):
    for item in data:
        for key in datetime_keys:
            if key in item and item[key]:
                # Parse the datetime assuming it may or may not include 'Z'
                dt = datetime.fromisoformat(item[key].rstrip('Z'))
                # Ensure the datetime is UTC and format to ISO 8601 without 'Z'
                dt = dt.replace(tzinfo=timezone.utc)  # Normalize to UTC
                item[key] = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # MariaDB DATETIME(3) compatible

            

for files in filenames:
    with open(files, 'r') as file:
        data = json.load(file)
        parser = ProcessDataParser(data)
        result = parser.parse()


        # Columns to ignore
        columns_to_ignore = {'input_dp_id', 'input_dp'}

        # Define renaming rules
        rename_columns = {
            'submit': 'submit_time',
            'start': 'start_time',
            'complete': 'end_time',
            'attempt': 'attempt_number',
            'process_uuid': 'id',
            'status': 'process_status'
        }

        # Desired order of columns
        column_order = [
            'id', 'pipeline_id', 'hardware_id', 'submit_time', 'start_time', 'end_time',
            'process_status', 'attempt_number', 'max_attempts', 'peasoup_id', 'pulsarx_id',
            'prepfold_id', 'filtool_id', 'circular_orbit_search_id', 'elliptical_orbit_search_id',
            'rfifind_id', 'candidate_filter_id', 'execution_order', 'program_name'
        ]

        # Renaming and filtering columns
        renamed_result = [{rename_columns.get(k, k): v for k, v in item.items() if k not in columns_to_ignore} for item in result]

        

        datetime_keys = ['submit_time', 'start_time', 'end_time']
        format_datetime(renamed_result, datetime_keys)

        # Determine which columns are present in the data
        columns_to_use = [col for col in column_order if col in set().union(*(d.keys() for d in renamed_result))]
        fill_missing_keys(renamed_result, columns_to_use)
        

        # Writing to CSV
        with open('processing_message.csv', 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=columns_to_use)
            writer.writeheader()
            writer.writerows(renamed_result)

       
        # Kafka producer command execution
        topic = "processing"
        value_file = "processing_message.csv"
        bootstrap_servers = "localhost:9092"
        schema_registry_url = "http://localhost:8081"
        schema_file = "avro_schema/Processing.avsc"
        cmds = f"python kafka_producer.py --bootstrap_servers {bootstrap_servers} --schema_registry_url {schema_registry_url} --schema_file {schema_file} --topic {topic} --value_file {value_file}"


        subprocess.run(cmds, shell=True)
        
        
