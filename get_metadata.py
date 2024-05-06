import your
import argparse
import os
import sys


def get_metadata_of_file(data):
    """
    Extract metadata from raw data.

    Parameters:
    - data: File path.

    Returns:
    - Dictionary containing metadata for the file.
    """
    metadata = []
    header = your.Your(data).your_header
    central_freq = header.center_freq
    bandwidth = header.bw
    lowest_freq = central_freq - bandwidth/2
    highest_freq = central_freq + bandwidth/2

    metadata = {
        "filename": os.path.basename(header.filename),
        "filepath": os.path.dirname(header.filename),
        "tstart_utc": header.tstart_utc.replace("T", "-"),
        "tsamp": header.tsamp,
        "tobs": header.tsamp * header.nspectra,
        "nsamples": header.nspectra,
        "freq_start_mhz": lowest_freq,
        "freq_end_mhz": highest_freq,
        "nchans": header.nchans,
        "nbits": header.nbits,
        "tstart": header.tstart,
        "foff": header.foff
    }
    return metadata

def main():
    parser = argparse.ArgumentParser(description='Get metadata from a file')
    parser.add_argument('-f', '--file', type=str, help='File to get metadata from', required=True)
    args = parser.parse_args()
    
    metadata = get_metadata_of_file(args.file)
  
    output_keys = ['tsamp', 'tobs', 'nsamples', 'freq_start_mhz', 'freq_end_mhz', 'tstart', 'tstart_utc', 'foff', 'nchans', 'nbits']

    for key in output_keys:
        print(f"{key}={metadata[key]}")
        


if __name__ == "__main__":
    main()

