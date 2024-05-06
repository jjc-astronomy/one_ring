import argparse
import csv
import logging
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import uuid_utils
import sys
# Set up logging
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

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
        print(self.bootstrap_servers)
        
        producer_conf = {
            'bootstrap.servers': self.bootstrap_servers,
        }
        return Producer(producer_conf)

    def _init_serializer(self):
        schema_registry_client = SchemaRegistryClient({"url": self.schema_registry_url})
        return AvroSerializer(schema_str=self.schema_str, schema_registry_client=schema_registry_client)

    def _serialize(self, value, topic):
        ctx = SerializationContext(topic, MessageField.VALUE)
        return self.value_serializer(value, ctx)

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            logging.error(f"Delivery failed for User record {msg.key()}: {err}")
            return
        logging.info(f'User record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

    def produce_message(self, topic, value):
        value_bytes = self._serialize(value, topic)
        self.producer.produce(topic=topic, value=value_bytes, on_delivery=self.delivery_report)
        self.producer.flush()
    
    @staticmethod
    def convert_row_types(row):
        int_fields = ['pipeline_id', 'hardware_id', 'filtool_id', 'execution_order', 'attempt_number', 'max_attempts']
        binary_fields = ['id', 'dp_id', 'processing_id']
        for field in int_fields:
            if field in row:
                row[field] = int(row[field])

        # Convert the 'id' field to binary for DB Insertion
        for field in binary_fields:
            if field in row:
                row[field] = uuid_utils.convert_uuid_string_to_binary(row[field])
        return row

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produce messages to Kafka topic.")
    parser.add_argument("--bootstrap_servers", required=True, help="Bootstrap servers for the Kafka cluster.")
    parser.add_argument("--schema_registry_url", required=True, help="URL for the Schema Registry.")
    parser.add_argument("--schema_file", required=True, help="Path to the Avro schema file.")
    parser.add_argument("--topic", required=True, help="Topic to produce the messages to.")
    parser.add_argument('--value_file', required=True, help='File containing the message values in CSV format.')

    args = parser.parse_args()

    kafka_producer = KafkaProducer(args.bootstrap_servers, args.schema_registry_url, args.schema_file)
    
    with open(args.value_file, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            row = kafka_producer.convert_row_types(row)
            kafka_producer.produce_message(args.topic, row)
          

    logging.info("Message production completed.")
