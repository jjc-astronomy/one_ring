#!/bin/bash

# Function to print help message
show_help() {
    echo "Usage: $0 -c <csv_file> -m <metadata_file> -p <pointing_idx>"
    echo
    echo "COMPACT DATABASE UPLOAD: Uploads list of raw files + metadata from the CSV and pipeline settings from Nextflow configs to the database. WARNING: Ensure 'hercules.config' and 'nextflow.config' are set before running."
    echo
    echo "Arguments:"
    echo "  -c, --csv           Path to the CSV file for uploading (required)"
    echo "  -m, --metadata      Path to the metadata file (required)"
    echo "  -p, --pointing-idx  Pointing index value (required)"
    echo "  -h, --help          Show this help message and exit"
    exit 0
}

# Check if running on 'csched'
if [[ "$(hostname)" != "tooarrana1" ]]; then
    echo "Error: This script must be run on the 'tooarrana1' machine."
    exit 1
fi

# If no arguments are provided, show help
if [[ $# -eq 0 ]]; then
    show_help
fi

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -c|--csv)
            csv_file="$2"
            shift 2
            ;;
        -m|--metadata)
            metadata_file="$2"
            shift 2
            ;;
        -p|--pointing-idx)
            pointing_idx="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            ;;
    esac
done

# Validate required arguments
if [[ -z "$csv_file" || -z "$metadata_file" || -z "$pointing_idx" ]]; then
    echo "Error: Missing required arguments."
    show_help
fi

# Validate file existence
if [[ ! -f "$csv_file" ]]; then
    echo "Error: CSV file '$csv_file' does not exist."
    exit 1
fi

if [[ ! -f "$metadata_file" ]]; then
    echo "Error: Metadata file '$metadata_file' does not exist."
    exit 1
fi

# Validate pointing index is a number
if ! [[ "$pointing_idx" =~ ^[0-9]+$ ]]; then
    echo "Error: Pointing index must be a numeric value."
    exit 1
fi

echo "Running upload process..."
echo "CSV File: $csv_file"
echo "Metadata File: $metadata_file"
echo "Pointing Index: $pointing_idx"

singularity_image_path="/fred/oz002/vishnu/singularity_images"
hardware_name="ngarrgu"
apptainer_image="${singularity_image_path}/skyweaverpy_latest.sif"
mounts="-B /fred"
apptainer_cmd="apptainer exec $mounts $apptainer_image"
upload_script="scripts/upload_data.py"

#Deleting docker image hash!
rm -rf docker_image_digests.csv
#First run Get docker image hash script
./get_docker_image_hash.sh

run_script="$apptainer_cmd python $upload_script --csv_file $csv_file --metadata_file $metadata_file --pointing_idx $pointing_idx --hardware_name ${hardware_name} --verbose"
# Add your upload processing logic here
source ~/.bashrc


echo "Setting up the nextflow config file"
set_up_cfg_file="nextflow config -profile ${hardware_name} -flat -sort > nf_config_for_data_upload.cfg"
echo "Running the command: $set_up_cfg_file"
eval $set_up_cfg_file

echo "Nextflow config file created"
echo "Running the command: $run_script"
$run_script
echo "Upload completed successfully."

