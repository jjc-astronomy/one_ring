import yaml
import json
import os, sys
import subprocess

# Function to read the YAML file
def read_yaml(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

# Function to parse the JSON file
def parse_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

# Function to run a command in a detached screen session
def run_command_in_screen(command, screen_name, auto_terminate=False):
    screen_command = f"screen -S {screen_name} -dm bash -c 'source ~/.bashrc && {command}; exec bash'"
    if auto_terminate:
        screen_command = f"screen -S {screen_name} -dm bash -c 'source ~/.bashrc && {command}'"
    result = subprocess.run(screen_command, shell=True, capture_output=True, text=True)
    

# Function to handle new_ozstar
def handle_new_ozstar(task):
    json_file = task['json_file']
    data_dir = task['data_dir']
    code_dir = task['code_dir']
    
    # Parse the JSON file
    parsed_json = parse_json(json_file)
    
    # Find the data_products.filename and update paths
    for program in parsed_json['programs']:
        for dp in program['data_products']:
            dp_filename = dp['filename']
            dp_basename = os.path.basename(dp_filename)
            new_path = os.path.join(data_dir, dp_basename)
            
            # Rsync the original data product file to the remote cluster
            rsync_dp_command = f"rsync -Pav {dp_filename} new_ozstar:{data_dir}"
            print(f"Running command: {rsync_dp_command}")  # Debugging output
            subprocess.run(rsync_dp_command, shell=True)
            
            # Update the JSON file with the new path
            dp['filename'] = new_path

    # Create a new filename for the updated JSON file
    updated_json_filename = os.path.splitext(json_file)[0] + "_updated.json"
    updated_json_path = os.path.join(os.path.dirname(json_file), os.path.basename(updated_json_filename))
    parsed_json['json_db_ids_filename'] = updated_json_filename

    # Save the updated JSON file locally
    with open(updated_json_path, 'w') as file:
        json.dump(parsed_json, file, indent=4)

    # Rsync the updated JSON file to the remote cluster code_dir
    rsync_command = f"rsync -Pav ./{updated_json_path} new_ozstar:{code_dir}"
    subprocess.run(rsync_command, shell=True)

    # Command to run on the remote cluster
    remote_command = f"cd {code_dir} && nextflow run main.nf -profile nt -params-file {os.path.basename(updated_json_path)}"
    ssh_command = f"ssh new_ozstar 'screen -S new_ozstar_nextflow -dm bash -c \"source ~/.bashrc && {remote_command}\"'"
    
    # Run the command in a remote screen session
    result = subprocess.run(ssh_command, shell=True, capture_output=True, text=True)
    

# Function to run nextflow command on contra
def handle_contra(task):
    json_file = task['json_file']
    code_dir = task['code_dir']
    command = f"cd {code_dir} && nextflow run main.nf -profile contra -params-file {json_file}"
    run_command_in_screen(command, "contra_nextflow")

def main():
    # Read the YAML file
    yaml_data = read_yaml('rsync_plan.yaml')
    
    # Iterate through the rsync_plan
    for task in yaml_data['rsync_plan']:
        hpc_cluster = task['hpc_cluster']
        
        if hpc_cluster == 'contra':
            handle_contra(task)
        elif hpc_cluster == 'new_ozstar':
            handle_new_ozstar(task)

if __name__ == "__main__":
    main()
