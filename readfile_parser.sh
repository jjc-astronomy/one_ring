#!/bin/bash

# Function to display help
show_help() {
  echo "Usage: ./readfile_parser.sh -f OBS_FILE"
  echo
  echo "Parse the output of the readfile command on a given observation file."
  echo
  echo "Options:"
  echo "  -f, --file    Path to the observation file."
  echo "  -h, --help    Display this help message."
}

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -h|--help)
      show_help
      exit 0
      ;;
    -f|--file)
      obs_file="$2"
      shift
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Check if the file argument was provided
if [[ -z "$obs_file" ]]; then
  echo "The -f or --file option must be provided."
  exit 1
fi

# Run readfile and capture its output
readfile_output=$(readfile "$obs_file")

# Parse readfile output
while IFS= read -r line; do
  trimmed=$(echo "$line" | awk '{$1=$1};1')

  case "$trimmed" in
    # "Tracking?"*|\
    # "Azimuth (deg)"*|\
    # "Zenith Ang (deg)"*|\
    # "Number of polns"*|\
    # "Beam ="*)
        # Ignore these lines
    #    ;;
    "bytes in file header ="*) ;;
    "Telescope ="*) telescope="${trimmed#Telescope = }" ;;
    "Source Name ="*) source_name="${trimmed#Source Name = }" ;;
    "Obs Date String ="*)
      obs_date_string_raw="${trimmed#Obs Date String = }"
      obs_date_string="${obs_date_string_raw//T/-}"
      ;;
    "MJD start time ="*) mjd_start_time="${trimmed#MJD start time = }" ;;
    "RA J2000 ="*) ra_j2000="${trimmed#RA J2000 = }" ;;
    "RA J2000 (deg) ="*) ra_j2000_deg="${trimmed#RA J2000 (deg) = }" ;;
    "Dec J2000 ="*) dec_j2000="${trimmed#Dec J2000 = }" ;;
    "Dec J2000 (deg) ="*) dec_j2000_deg="${trimmed#Dec J2000 (deg) = }" ;;
    "Sample time (us) ="*) sample_time="${trimmed#Sample time (us) = }" ;;
    "Central freq (MHz) ="*) central_freq="${trimmed#Central freq (MHz) = }" ;;
    "Low channel (MHz) ="*) low_channel="${trimmed#Low channel (MHz) = }" ;;
    "High channel (MHz) ="*) high_channel="${trimmed#High channel (MHz) = }" ;;
    "Channel width (MHz) ="*) channel_width="${trimmed#Channel width (MHz) = }" ;;
    "Number of channels ="*) num_channels="${trimmed#Number of channels = }" ;;
    "Total Bandwidth (MHz) ="*) total_bandwidth="${trimmed#Total Bandwidth (MHz) = }" ;;
    "Spectra per file ="*) spectra_per_file="${trimmed#Spectra per file = }" ;;
    "Time per file (sec) ="*) time_per_file="${trimmed#Time per file (sec) = }" ;;
    "bits per sample ="*) bits_per_sample="${trimmed#bits per sample = }" ;;
   # *) echo "Ignored line: $trimmed" ;;
  esac
done <<< "$readfile_output"

# # Print the parsed values
# echo "Telescope: $telescope"
# echo "Source Name: $source_name"
# echo "Obs Date String: $obs_date_string"
# echo "MJD start time: $mjd_start_time"
# echo "RA J2000: $ra_j2000"
# echo "RA J2000 (deg): $ra_j2000_deg"
# echo "Dec J2000: $dec_j2000"
# echo "Dec J2000 (deg): $dec_j2000_deg"
# echo "Sample time (us): $sample_time"
# echo "Central freq (MHz): $central_freq"
# echo "Low channel (MHz): $low_channel"
# echo "High channel (MHz): $high_channel"
# echo "Channel width (MHz): $channel_width"
# echo "Number of channels: $num_channels"
# echo "Total Bandwidth (MHz): $total_bandwidth"
# echo "bits per sample: $bits_per_sample"
# echo "Total samples in Observation: $spectra_per_file"
# echo "Tobs (sec): $time_per_file"

#echo as csv
echo "$telescope,$source_name,$obs_date_string,$mjd_start_time,$ra_j2000,$dec_j2000,$ra_j2000_deg,$dec_j2000_deg,$sample_time,$central_freq,$low_channel,$high_channel,$channel_width,$num_channels,$total_bandwidth,$bits_per_sample,$spectra_per_file,$time_per_file" 


