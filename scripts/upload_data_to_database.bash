#!/usr/bin/env bash
set -euo pipefail

# === Function: help ===
show_help() {
    echo "Usage: $0 -c <csv_file> -m <metadata_file> -p <pointing_idx> [--hardware-name contra|hercules|ozstar|ngarrgu]"
    echo
    echo "COMPACT DATABASE UPLOAD: Uploads raw files + metadata + Nextflow config."
    echo "Arguments:"
    echo "  -c, --csv           Path to CSV file (required)"
    echo "  -m, --metadata      Path to metadata file (required)"
    echo "  -p, --pointing-idx  Pointing index (numeric, required)"
    echo "  --hardware-name     Cluster name [default: contra]"
    echo "  -h, --help          Show this help message and exit"
    exit 0
}

# === Defaults ===
hardware_name="contra"

# === Parse args ===
if [[ $# -eq 0 ]]; then show_help; fi

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
        --hardware-name)
            hardware_name="$2"
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

# === Validate required ===
[[ -z "${csv_file:-}" || -z "${metadata_file:-}" || -z "${pointing_idx:-}" ]] && {
    echo "Error: -c, -m, and -p are required."
    show_help
}

[[ -f "$csv_file" ]] || { echo "Error: CSV file '$csv_file' does not exist."; exit 1; }
[[ -f "$metadata_file" ]] || { echo "Error: Metadata file '$metadata_file' does not exist."; exit 1; }
[[ "$pointing_idx" =~ ^[0-9]+$ ]] || { echo "Error: Pointing index must be numeric."; exit 1; }

# === Repo root ===
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(realpath "$SCRIPT_DIR/..")"
SCRATCH_DIR="$REPO_ROOT/scratch"
mkdir -p "$SCRATCH_DIR"

echo "Detected repo root: $REPO_ROOT"
[[ -f "$REPO_ROOT/nextflow.config" ]] || {
    echo "Error: Cannot find nextflow.config in $REPO_ROOT"
    exit 1
}

# === Hardware config ===
declare -A SINGULARITY_IMAGE_PATHS
declare -A MOUNTS
declare -A HOSTNAME_PREFIXES

SINGULARITY_IMAGE_PATHS["contra"]="/b/u/vishnu/SINGULARITY_IMAGES"
MOUNTS["contra"]="-B /homes -B /b -B /bscratch"
HOSTNAME_PREFIXES["contra"]="csched"

SINGULARITY_IMAGE_PATHS["hercules"]="/hercules/scratch/vishnu/singularity_images"
MOUNTS["hercules"]="-B /hercules -B /mandap"
HOSTNAME_PREFIXES["hercules"]="hercules"

SINGULARITY_IMAGE_PATHS["ozstar"]="/fred/oz002/vishnu/singularity_images"
MOUNTS["ozstar"]="-B /fred"
HOSTNAME_PREFIXES["ozstar"]="farnarkle"

SINGULARITY_IMAGE_PATHS["ngarrgu"]="/fred/oz002/vishnu/singularity_images"
MOUNTS["ngarrgu"]="-B /fred"
HOSTNAME_PREFIXES["ngarrgu"]="tooarrana"

# === Validate hardware_name ===
if [[ -z "${SINGULARITY_IMAGE_PATHS[$hardware_name]+x}" ]]; then
    echo "Error: Unknown hardware_name '$hardware_name'. Valid: ${!SINGULARITY_IMAGE_PATHS[*]}"
    exit 1
fi

# === Validate host ===
expected_prefix="${HOSTNAME_PREFIXES[$hardware_name]}"
actual_hostname="$(hostname)"
if [[ "$actual_hostname" != "$expected_prefix"* ]]; then
    echo "Error: For '$hardware_name', hostname must start with '$expected_prefix'."
    echo "Current hostname: $actual_hostname"
    exit 1
fi

echo "Host validated: $actual_hostname matches $expected_prefix*"

# === Resolve paths ===
singularity_image_path="${SINGULARITY_IMAGE_PATHS[$hardware_name]}"
mounts="${MOUNTS[$hardware_name]}"
apptainer_image="${singularity_image_path}/skyweaverpy_latest.sif"

# === Load module if needed ===
if [[ "$hardware_name" == "ozstar" || "$hardware_name" == "ngarrgu" ]]; then
    echo "Loading apptainer module for $hardware_name..."
    module load apptainer
fi

apptainer_cmd="apptainer exec $mounts $apptainer_image"
upload_script="scripts/upload_data.py"

echo "Hardware: $hardware_name"
echo "Image: $apptainer_image"
echo "Mounts: $mounts"

# === Create Nextflow config ===
cd "$REPO_ROOT"
NF_CONFIG_FILE="$SCRATCH_DIR/nf_config_for_data_upload.cfg"
echo "Generating Nextflow flat config: $NF_CONFIG_FILE"
echo "Running command: nextflow config -profile $hardware_name -flat -sort > "$NF_CONFIG_FILE""
nextflow config -profile $hardware_name -flat -sort > "$NF_CONFIG_FILE"
echo "Nextflow config generated."

# === Get Docker digests ===
DOCKER_HASH_FILE="$SCRATCH_DIR/docker_image_digests.csv"
echo "Running docker image hash script..."
bash "$SCRIPT_DIR/get_docker_image_hash.sh"
echo "Docker digests written to $DOCKER_HASH_FILE"

# === Source env ===
source ~/.bashrc

# === Build and run upload ===
echo "Running upload..."
run_script="$apptainer_cmd python $REPO_ROOT/$upload_script \
  --csv_file $csv_file \
  --metadata_file $metadata_file \
  --pointing_idx $pointing_idx \
  --nextflow_cfg $NF_CONFIG_FILE \
  --docker_hash_file $DOCKER_HASH_FILE \
  --hardware_name $hardware_name \
  --verbose"

echo "Command:"
echo "$run_script"
eval "$run_script"

echo "Upload completed successfully!"

