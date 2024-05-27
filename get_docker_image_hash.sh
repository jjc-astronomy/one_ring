#!/bin/bash

# Define images and their versions
declare -A IMAGES
IMAGES["vishnubk/compact_sql"]="latest"
IMAGES["vishnubk/peasoup"]="latest"
IMAGES["vishnubk/pulsar-miner"]="turing-sm75"
IMAGES["vishnubk/pulsarx"]="latest"

#apptainer pull docker://vishnubk/peasoup:latest -> .sif (hash from dockerhub -> docker image)
# Output file
OUTPUT_FILE="docker_image_digests.csv"

# Check if output file exists, if not create it with headers
if [ ! -f "$OUTPUT_FILE" ]; then
    echo "Username,Image,Version,SHA256" > "$OUTPUT_FILE"
fi

# Function to get digest
get_digest() {
    local image=$1
    local version=$2
    TOKEN=$(curl -s "https://auth.docker.io/token?service=registry.docker.io&scope=repository:$image:pull" | python -c "import sys, json; print(json.load(sys.stdin)['token'])")
    DIGEST=$(curl -s -H "Authorization: Bearer $TOKEN" -H "Accept: application/vnd.docker.distribution.manifest.v2+json" -I "https://registry-1.docker.io/v2/$image/manifests/$version" | grep docker-content-digest | awk '{print $2}')
    echo ${DIGEST#"sha256:"}
}

# Iterate over images and versions
for image_with_namespace in "${!IMAGES[@]}"; do
    version=${IMAGES[$image_with_namespace]}
    # Split image name into username and image
    IFS='/' read -r username image <<< "$image_with_namespace"
    # Check if entry is already in the file
    if ! grep -q "$username,$image,$version," "$OUTPUT_FILE"; then
        # Get digest
        digest=$(get_digest $image_with_namespace $version)
        # Append username, image, version, and digest to file
        echo "$username,$image,$version,$digest" >> "$OUTPUT_FILE"
    fi
done

echo "Digests stored/updated in $OUTPUT_FILE"
