#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import subprocess
import pandas as pd
import numpy as np
import shlex
import threading
import time
import glob

###############################################################################
# Logging Setup
###############################################################################
def setup_logging(verbose=False):
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")

###############################################################################
# Output Streaming with Buffer (used for psrfold_fil logs)
###############################################################################
def buffered_stream_output(pipe, logger, log_level=logging.INFO, flush_interval=1.0):
    """
    Reads lines from 'pipe' and logs them every 'flush_interval' seconds.
    All lines are flushed at the end, ensuring none are skipped.
    """
    buffer = []
    last_flush = time.time()
    try:
        for line in iter(pipe.readline, ''):
            buffer.append(line)
            if (time.time() - last_flush) >= flush_interval:
                for msg in buffer:
                    logger.log(log_level, msg.rstrip('\n'))
                buffer.clear()
                last_flush = time.time()

        # Final flush
        for msg in buffer:
            logger.log(log_level, msg.rstrip('\n'))
        buffer.clear()
    finally:
        pipe.close()

###############################################################################
# Spin Conversions
###############################################################################
def calculate_spin(f=None, fdot=None, p=None, pdot=None):
    """
    Helper to convert (f, fdot) <-> (p, pdot).
    """
    if f is not None and fdot is not None:
        p = 1 / f
        pdot = -fdot / (f ** 2)
    elif p is not None and pdot is not None:
        f = 1 / p
        fdot = -pdot * (p ** 2)
    else:
        raise ValueError("Either (f, fdot) or (p, pdot) must be provided")
    return f, fdot, p, pdot

###############################################################################
# Create .candfile for PulsarX at DM=0
###############################################################################
def generate_zero_dm_cand_file(df_candidates, output_candfile="refold_zero_dm.candfile"):
    """
    Generate a .candfile for PulsarX re-folding at DM=0. 
    We rely on df_candidates having:
      - 'temp_row_index_for_refold' (0..N-1) => #id in .candfile will be +1
      - columns like 'f0_old', 'f1_old', 'acc_old', 'dm_old', 'S/N' etc.
        (you can adapt if your columns differ)
    DM is forced to 0.0 here, so that psrfold_fil folds at zero DM.
    """
    with open(output_candfile, 'w') as fout:
        fout.write("#id DM accel F0 F1 F2 S/N\n")
        for _, row in df_candidates.iterrows():
            # .candfile #id = row_index + 1
            cand_id_psrfold = row['temp_row_index_for_refold'] + 1

            # Force DM=0 here
            dm = 0.0  
            # If you want to preserve acceleration from the original row:
            acc = row.get('acc_old', 0.0)  
            f0 = row.get('f0_old', 0.0)
            f1 = 0.0
            f2 = 0.0
            snr = row.get('S/N', 0.0)  # Original S/N from the XML

            fout.write(f"{cand_id_psrfold} {dm} {acc} {f0} {f1} {f2} {snr}\n")

    return output_candfile

###############################################################################
# Run psrfold_fil with the zero-DM .candfile
###############################################################################
def run_psrfold_fil_zero_dm(
    candfile, 
    output_rootname,
    pepoch,
    start_fraction,
    end_fraction,
    input_filenames,
    beam_name,
    subint_length,
    nsubband,
    nbins_low,
    nbins_high,
    pulsarx_threads,
    template,
    clfd_q_value,
    rfi_filter=None,
    zap_string=None,
    extra_args=None
):
    """
    Invokes psrfold_fil in a single shot for all above-threshold candidates.
    The newly created .cands file will be named something like <output_rootname>.cands
    """
    if 'ifbf' in beam_name:
        beam_tag = "--incoherent"
    elif 'cfbf' in beam_name:
        beam_tag = f"-i {int(beam_name.strip('cfbf'))}"
    else:
        beam_tag = ""

    nbins_string = f"-b {nbins_low} --nbinplan 0.1 {nbins_high}"
    rfi_flags = f"--rfi {rfi_filter}" if rfi_filter else ""
    zap_string = zap_string if zap_string else ""

    cmd = (
        f"psrfold_fil -v --nosearch "  
        f"-t {pulsarx_threads} "
        f"--candfile {candfile} "
        f"-n {nsubband} {nbins_string} {beam_tag} "
        f"--template {template} "
        f"--clfd {clfd_q_value} "
        f"-L {subint_length} "
        f"-f {input_filenames} "
        f"{zap_string} "
        f"-o {output_rootname} "
        f"--srcname zero_dm_fold "
        f"--pepoch {pepoch} "
        f"--frac {start_fraction} {end_fraction} "
        f"{rfi_flags}"
    )

    if extra_args:
        cmd += f" {extra_args}"

    logging.info("Running psrfold_fil at DM=0 on multiple candidates.")
    logging.debug(f"CMD: {cmd}")

    process = subprocess.Popen(
        shlex.split(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )

    stdout_thread = threading.Thread(
        target=buffered_stream_output,
        args=(process.stdout, logging.getLogger(), logging.INFO, 10.0),
        daemon=True
    )
    stderr_thread = threading.Thread(
        target=buffered_stream_output,
        args=(process.stderr, logging.getLogger(), logging.WARNING, 10.0),
        daemon=True
    )

    stdout_thread.start()
    stderr_thread.start()
    stdout_thread.join()
    stderr_thread.join()

    return_code = process.wait()
    if return_code != 0:
        logging.error(f"psrfold_fil returned non-zero exit status {return_code}")
        sys.exit(1)

    return True

###############################################################################
# Read the newly created .cands file
###############################################################################
def parse_zero_dm_cands_file(cands_file):
    """
    Reads the .cands file produced by psrfold_fil. 
    Typically skip the first ~11 lines as they are headers.
    """
    df = pd.read_csv(cands_file, skiprows=11, sep=r'\s+')
    return df

###############################################################################
# Main script
###############################################################################
def main():
    parser = argparse.ArgumentParser(
        description="Refold candidates above threshold at DM=0, then compute alpha/beta/gamma."
    )
    parser.add_argument("-i", "--input_csv", required=True, 
                        help="Path to the search_fold_merged.csv file.")
    parser.add_argument("-o", "--output_csv", default="final_merged.csv", 
                        help="Final output CSV after zero-DM refolding.")
    parser.add_argument("--threshold", type=float, default=9.0, 
                        help="S/N_new threshold for re-fold at DM=0.")
    parser.add_argument("--pulsarx_threads", type=int, default=8, 
                        help="Threads for psrfold_fil.")
    parser.add_argument("--nsubband", type=int, default=64, 
                        help="Number of subbands.")
    parser.add_argument("--nbins_low", type=int, default=64, 
                        help="Lower profile bin limit.")
    parser.add_argument("--nbins_high", type=int, default=128, 
                        help="Upper profile bin limit.")
    parser.add_argument("--subint_length", type=int, default=10, 
                        help="Subint length in seconds.")
    parser.add_argument("--template", type=str, default="meerkat_fold.template", 
                        help="Fold template.")
    parser.add_argument("--clfd_q_value", type=float, default=2.0, 
                        help="CLFD Q value.")
    parser.add_argument("--rfi_filter", type=str, default=None, 
                        help="RFI filter value.")
    parser.add_argument("--beam_name", type=str, default="cfbf00000", 
                        help="Beam name.")
    parser.add_argument("--original_filenames", type=str, default="input.fil", 
                        help="Filterbank or file(s) to pass to psrfold_fil.")
    parser.add_argument("--pepoch", type=float, default=59000.0, 
                        help="Pepoch. Override or from CSV if needed.")
    parser.add_argument("--start_frac", type=float, default=0.0, 
                        help="Start fraction for psrfold_fil.")
    parser.add_argument("--end_frac", type=float, default=1.0, 
                        help="End fraction for psrfold_fil.")
    parser.add_argument("--extra_args", type=str, default=None, 
                        help="Additional arguments for psrfold_fil.")
    parser.add_argument("--candfile_output", type=str, default="refold_zero_dm.candfile", 
                        help="Output .candfile name for zero-DM folding.")
    parser.add_argument("--cands_file_output", type=str, default="refold_zero_dm.cands", 
                        help="Output root name for the new .cands file from psrfold_fil.")
    parser.add_argument("--zap_string", type=str, default="", 
                        help="Zap string for RFI channels, e.g. '--rfi zap 100 200'.")
    parser.add_argument("--verbose", action='store_true', 
                        help="Enable verbose logging.")

    args = parser.parse_args()
    setup_logging(verbose=args.verbose)

    ###########################################################################
    # 1) Load original merged CSV
    ###########################################################################
    logging.info(f"Reading input CSV: {args.input_csv}")
    df = pd.read_csv(args.input_csv)

    #Calculate beta and gamma.
    if 'dm_old' in df.columns and 'dm_new' in df.columns and 'dm_err' in df.columns:
            df['beta'] = (df['dm_old'] - df['dm_new']).abs() / df['dm_old']
            df['gamma'] = df['dm_err'] / df['dm_new']
    else:
        df['beta'] = np.nan
        df['gamma'] = np.nan

    # Store the original index in a column for later re-alignment
    # This allows us to place alpha back exactly in the correct rows.
    df['orig_index'] = df.index
    
    
    

    ###########################################################################
    # 2) Identify rows above threshold
    ###########################################################################
    logging.info(f"Filtering rows by S/N_new > {args.threshold}")
    df_above_threshold = df[df['S/N_new'] > args.threshold].copy()
    

    # If none pass threshold, fill alpha/beta/gamma with NaN for all
    if df_above_threshold.empty:
        logging.info("No candidates above threshold. Nothing to refold at DM=0.")
        df['alpha'] = np.nan
       
        df.to_csv(args.output_csv, index=False)
        logging.info(f"Wrote {args.output_csv}.")
        sys.exit(0)

    ###########################################################################
    # 3) Assign a small index for re-folding
    ###########################################################################
    # We'll reset index on the above-threshold subset so we have a clean 0..N-1
    df_above_threshold.reset_index(drop=True, inplace=True)
    # This new index is used to map lines in the .candfile -> lines in the .cands
    df_above_threshold['temp_row_index_for_refold'] = np.arange(len(df_above_threshold))

    
    ###########################################################################
    # 4) Generate a single .candfile with DM=0 lines for those above-threshold
    ###########################################################################
    logging.info(f"Creating zero-DM candfile: {args.candfile_output}")
    generate_zero_dm_cand_file(df_above_threshold, output_candfile=args.candfile_output)

    ###########################################################################
    # 5) Run psrfold_fil at DM=0
    ###########################################################################
    logging.info("Running psrfold_fil for all above-threshold candidates at DM=0...")
    run_psrfold_fil_zero_dm(
        candfile=args.candfile_output,
        output_rootname=args.cands_file_output,
        pepoch=args.pepoch,
        start_fraction=args.start_frac,
        end_fraction=args.end_frac,
        input_filenames=args.original_filenames,
        beam_name=args.beam_name,
        subint_length=args.subint_length,
        nsubband=args.nsubband,
        nbins_low=args.nbins_low,
        nbins_high=args.nbins_high,
        pulsarx_threads=args.pulsarx_threads,
        template=args.template,
        clfd_q_value=args.clfd_q_value,
        rfi_filter=args.rfi_filter,
        zap_string=args.zap_string,
        extra_args=args.extra_args
    )

    ###########################################################################
    # 6) Locate the newly created .cands file
    ###########################################################################
    # psrfold_fil appends ".cands". We'll find it by globbing.
    possible_cands = glob.glob(f"{args.cands_file_output}*.cands")
    if not possible_cands:
        logging.error(f"Could not find .cands file from output root '{args.cands_file_output}'")
        sys.exit(1)
    zero_dm_cands_filename = possible_cands[0]
    logging.info(f"Parsing newly created .cands file: {zero_dm_cands_filename}")

    ###########################################################################
    # 7) Parse the zero-DM .cands file
    ###########################################################################
    df_zero_dm = parse_zero_dm_cands_file(zero_dm_cands_filename)

    # By default, #id in that file runs from 1..N in the same order 
    # as lines in the .candfile. So #id - 1 => temp_row_index_for_refold
    df_zero_dm['temp_row_index_for_refold'] = df_zero_dm['#id'] - 1

    # We'll rename the S/N column so we don't confuse it with the original
    # Typically the new .cands file might have 'S/N_new' or just 'S/N'
    # We'll treat whichever we find.
    if 'S/N_new' in df_zero_dm.columns:
        df_zero_dm.rename(columns={'S/N_new': 'S/N_zero_dm'}, inplace=True)
    elif 'S/N' in df_zero_dm.columns:
        df_zero_dm.rename(columns={'S/N': 'S/N_zero_dm'}, inplace=True)

    ###########################################################################
    # 8) Merge the zero-DM results back into df_above_threshold
    ###########################################################################
    # We only need a few columns from df_zero_dm. 
    # The new zero-DM S/N is 'S/N_zero_dm'.
    # For alpha calculation, we only need that. 
   
    columns_to_merge = ['temp_row_index_for_refold', 'S/N_zero_dm']
    if 'dm_new' in df_zero_dm.columns:
        df_zero_dm.rename(columns={'dm_new': 'dm_new_zero_dm'}, inplace=True)
        columns_to_merge.append('dm_new_zero_dm')

    df_above_threshold_merged = pd.merge(
        df_above_threshold,
        df_zero_dm[columns_to_merge],
        on='temp_row_index_for_refold',
        how='left'
    )

    # Compute alpha = (S/N_zero_dm) / (S/N_new). 
    # Only for these above-threshold rows. 
    df_above_threshold_merged['alpha'] = (
        df_above_threshold_merged['S/N_zero_dm'] / df_above_threshold_merged['S/N_new']
    )

    
    # Let's set an index on df_above_threshold_merged for direct alignment
    df_above_threshold_merged.set_index('orig_index', inplace=True)

    # Now set an index on the main df, and do assignment
    df_final = df.set_index('orig_index', drop=True)

    # Create columns alpha, beta, gamma if they don't exist
    if 'alpha' not in df_final.columns:
        df_final['alpha'] = np.nan
    if 'S/N_zero_dm' not in df_final.columns:
        df_final['S/N_zero_dm'] = np.nan

    # For the above-threshold subset, fill in the new values
    df_final.loc[df_above_threshold_merged.index, 'alpha'] = df_above_threshold_merged['alpha']
    df_final.loc[df_above_threshold_merged.index, 'beta']  = df_above_threshold_merged['beta']
    df_final.loc[df_above_threshold_merged.index, 'gamma'] = df_above_threshold_merged['gamma']
    # If you also want the new S/N at zero DM in the final CSV:
    if 'S/N_zero_dm' in df_above_threshold_merged.columns:
        df_final.loc[df_above_threshold_merged.index, 'S/N_zero_dm'] = df_above_threshold_merged['S/N_zero_dm']

   
    ###########################################################################
    # 10) Write final CSV
    ###########################################################################
    df_final.reset_index(drop=True, inplace=True)
    logging.info(f"Writing final CSV: {args.output_csv}")
    df_final.to_csv(args.output_csv, index=False)
    logging.info("Done.")

if __name__ == "__main__":
    main()
