#!/bin/bash

# Required input parameters
baseDir="$1"
process_uuid="$2"
submit_time="$3"
execution_order="$4"
jsonconfig="$5"
kafka_server="$6"
schema_registry="$7"
scratch="$8"
message_create_script="$9"
producer_script="${10}"

# Activate the virtual environment
source "${baseDir}/kafka_env/bin/activate"

# Record the end time
end_time=$(date -u +"%Y-%m-%dT%H:%M:%S")
echo "End time is: ${start_time}"
echo ${end_time} > ${scratch}/start_time.txt

# Create a message to be sent to Kafka
python ${message_create_script} --id ${process_uuid} --submit_time ${submit_time} --start_time ${start_time} --process_status RUNNING -p filtool -t processing -e ${execution_order} -j ${jsonconfig}

# Function to check port and set up SSH tunnel
setup_ssh_tunnel() {
    if ! netstat -tuln | grep -q ":9092 "; then
        # If port 9092 is not used, set up SSH port forwarding
        ssh -L 9092:127.0.0.1:9092 -L 8081:127.0.0.1:8081 -L 8083:127.0.0.1:8083 tooarrana1 -N -f
    fi
}

# Function to produce a message to Kafka
produce_message() {
    # Update Kafka server and schema registry URLs, temporarily they are the same.
    new_kafka_server=${kafka_server}
    new_schema_registry_url=${schema_registry}

    # Try to produce the message to Kafka
    python ${producer_script} --bootstrap_servers ${new_kafka_server} --schema_registry_url ${new_schema_registry_url} --schema_file ${baseDir}/avro_schema/Processing.avsc --topic processing --value_file processing_message_update.csv
}

# Retry logic
max_attempts=3
attempt=1
while [ $attempt -le $max_attempts ]; do
    echo "Attempt $attempt of $max_attempts"
    setup_ssh_tunnel
    produce_message && break  # Break the loop if produce_message is successful
    attempt=$(( $attempt + 1 ))
    sleep 10  # Wait for 10 seconds before retrying
done

# Check if all attempts failed
if [ $attempt -gt $max_attempts ]; then
    echo "Failed to produce message to Kafka after $max_attempts attempts."
fi

# Dynamic port selection (commented for potential future use)
# find_free_port() {
#     while true; do
#         PORT=$(shuf -i 2000-65000 -n 1)  # Randomize a port range
#         ! netstat -tuln | grep -q ":$PORT " && echo $PORT && break
#     done
# }
# kafka_port=$(find_free_port)
# schema_registry_port=$(find_free_port)
# connect_port=$(find_free_port)