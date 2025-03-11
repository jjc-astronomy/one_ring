import argparse
import json
import logging
from datetime import datetime, timedelta
import uuid_utils, csv
import pandas as pd


# Setup basic configuration for logging
#logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ProcessingMessage:
    """
    Creates a message dictionary for processing with fields compatible for insertion into the Processing Table of the Database.
    Handles UUID generation/conversion, datetime fields formatting, and dynamically includes any additional specified fields.

    Parameters:
    - pipeline_id (int, optional): ID of the pipeline. Not required if 'id' is provided.
    - hardware_id (int, optional): ID of the hardware. Not required if 'id' is provided.
    - program_name (str): Name of the program. Determines the required_program_id field.
    - submit_delay_seconds (int, optional): Delay in seconds to adjust the submit_time. Defaults to 1.
    - **kwargs: Additional fields to be included in the message, including 'id', 'start_time', 'end_time', 'process_status', etc.

    If 'id' is provided, it is used as the primary key, and 'pipeline_id' and 'hardware_id' become optional.
    If program_id does not match the provided program_name in kwargs, the class initialization will fail.
    """
    
    def __init__(self, program_name, submit_delay_seconds=1, **kwargs):
        id_provided = kwargs.pop('id', None)
        self.pipeline_id = kwargs.pop('pipeline_id', None)
        self.hardware_id = kwargs.pop('hardware_id', None)
        
        if id_provided:
            self.id = id_provided
            self.submit_time = kwargs.pop('submit_time', None)
            
        else:
            if pipeline_id is None or hardware_id is None:
                raise ValueError("Both 'pipeline_id' and 'hardware_id' are required when 'id' is not provided.")
            self.pipeline_id = pipeline_id
            self.hardware_id = hardware_id
            # Generate a UUID string if 'id' is not provided
            self.id = uuid_utils.generate_uuid_string()
            self.submit_time = kwargs.pop('submit_time', datetime.utcnow() + timedelta(seconds=submit_delay_seconds))


        self.program_name = program_name
        self.required_program_id = f"{program_name}_id"
        self.program_id = kwargs.pop(self.required_program_id, None)
        
        if self.required_program_id and not self.program_id:
            error_message = f"Required program ID '{self.required_program_id}' not provided."
            logging.error(error_message)
            raise ValueError(error_message)
        
        
        
        self.message_data = kwargs

    def return_message(self):
        data = {'id': self.id, 'program_name': self.program_name}
        
        # Optionally include 'pipeline_id' and 'hardware_id' if they are set
        if self.pipeline_id is not None:
            data['pipeline_id'] = self.pipeline_id
        if self.hardware_id is not None:
            data['hardware_id'] = self.hardware_id
        
        if self.submit_time is not None:
            data['submit_time'] = self.submit_time.isoformat()
        if self.program_id is not None:
            data[self.required_program_id] = self.program_id
        
        # Add any additional fields, ensuring datetime objects are formatted correctly
        for key, value in self.message_data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
            else:
                data[key] = value
        
        logging.info("Message creation successful.")
        return data
    
    def write_message_to_csv(self, filename, header=True):
        message = self.return_message()
        print(message)
        with open(filename, 'w', newline='') as csvfile:  
            fieldnames = message.keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            if header:  # Write header if specified
                writer.writeheader()
            writer.writerow(message)
            logging.info("Message written to CSV successfully.")
        
        return message


class ProcessingDPInputs:
    def __init__(self, processing_id, data_product_ids):
        """
        Initialize the ProcessingDPInputs with a processing ID and a list of data product IDs.

        :param processing_id: The processing ID to use for all entries.
        :param data_product_ids: A list of data product IDs to use for generating messages.
        """
        self.processing_id = processing_id
        self.data_product_ids = data_product_ids

    def generate_messages(self):
        """
        Generate a list of messages where each message contains a unique ID, a data product ID, and the processing ID.

        :return: List of tuples, each representing a message.
        """
        messages = []
        for dp_id in self.data_product_ids:
            msg_id = uuid_utils.generate_uuid_string()
            messages.append((msg_id, dp_id, self.processing_id))
        return messages

    def write_to_csv(self, filename):
        """
        Write the generated messages to a CSV file.

        :param filename: The name of the file where the data should be written.
        """
        messages = self.generate_messages()
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['id', 'dp_id', 'processing_id','topic'])  # Write CSV header
            for msg in messages:
                writer.writerow(msg)
        




# Define the parser and add arguments
parser = argparse.ArgumentParser(description="Parse JSON file.")
parser.add_argument("-j", "--json_file", default='raw_dp_with_ids.json', help="Path to the JSON file to be parsed.")
parser.add_argument("-p", "--program_name", required=True, help="Name of the program to generate message")
parser.add_argument("-e", "--execution_order", default=1, type=int, help="Execution order of the program")
parser.add_argument("-t", "--table", required=True, type=str, help='SQL table schema used to generate message')
parser.add_argument("--id", help="ID of the processing entry (in string format)")
parser.add_argument("--submit_time", help="Submit time of the process", default=None)
parser.add_argument("--start_time", help="Start time of the process", default=None)
parser.add_argument("--end_time", help="End time of the process", default=None)
parser.add_argument("--process_status", help="Status of the process", default=None)
parser.add_argument("--attempt_number", type=int, help="Attempt number of process", default=1)
parser.add_argument("--max_attempts", type=int, help="Max number of attempts for process", default=3)


# Parse command line arguments
args = parser.parse_args()
id_provided = args.id
#kafka_topic = args.table
# Open and parse the JSON file
with open(args.json_file, "r") as value_file:
    data_cfg = json.load(value_file)

pipeline_id = data_cfg['pipeline_id']
hardware_id = data_cfg['hardware_id']

# Build the dynamic key name
dynamic_key = f"{args.program_name}_id"

# Attempt to get the value using the dynamic key
program_id = data_cfg.get(dynamic_key)
execution_order = args.execution_order

if program_id is None:
    logging.error(f"Key '{dynamic_key}' not found in the JSON file.")
else:
    print(f"{dynamic_key}: {program_id}, pipeline_id: {pipeline_id}, hardware_id: {hardware_id}, execution_order: {execution_order}")


if id_provided:
    id_provided = args.id
    update_fields = {}
    update_fields[f"{args.program_name}_id"] = program_id

    if args.submit_time:
        update_fields['submit_time'] = datetime.fromisoformat(args.submit_time)
    if args.start_time:
        update_fields['start_time'] = datetime.fromisoformat(args.start_time)
    if args.end_time:
        update_fields['end_time'] = datetime.fromisoformat(args.end_time)
    if args.process_status:
        update_fields['process_status'] = args.process_status
    if args.attempt_number:
        update_fields['attempt_number'] = args.attempt_number
    if args.max_attempts:
        update_fields['max_attempts'] = args.max_attempts
    

    #Add pipeline and hardware id, if not already present. Needed for kafka connect upsert
    if 'pipeline_id' not in update_fields:
        update_fields['pipeline_id'] = pipeline_id
    if 'hardware_id' not in update_fields:
        update_fields['hardware_id'] = hardware_id
    if 'execution_order' not in update_fields:
        update_fields['execution_order'] = execution_order
    try:
        status_update = ProcessingMessage(program_name=args.program_name, id=id_provided, **update_fields)
        message_csv = status_update.write_message_to_csv("processing_message_update.csv", header=True)
        
    except ValueError as e:
        logging.error(f"Failed to update message: {e}")

else:
    if args.table.lower() == 'processing':
        try:
            dynamic_args = {f"{args.program_name}_id": program_id}
            dynamic_args['process_status'] = args.process_status
            message = ProcessingMessage(args.program_name, pipeline_id=pipeline_id, hardware_id=hardware_id,
                                        execution_order=execution_order, **dynamic_args)
            processing_messages = message.write_message_to_csv("processing_message.csv", header=True)
            # Creating message for processing_dp_inputs table.
            processing_id = processing_messages['id']
            dp_ids = [product['dp_id'] for product in data_cfg['data_products']]
            processing_dp_inputs = ProcessingDPInputs(processing_id, dp_ids)
            processing_dp_inputs.write_to_csv("process_dp_inputs_message.csv")
        except ValueError as e:
            logging.error(f"Failed to create message: {e}")


