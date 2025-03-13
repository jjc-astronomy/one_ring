import argparse
import logging
import os
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
from bestprof_utils import parse_bestprof
import subprocess
import sys
from uncertainties import ufloat
from uncertainties.unumpy import uarray, nominal_values, std_devs


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def a_to_pdot(P_s, acc_ms2):
    LIGHT_SPEED = 2.99792458e8  # Speed of Light in SI
    return P_s * acc_ms2 / LIGHT_SPEED

def calculate_spin(f=None, fdot=None, p=None, pdot=None):
    if f is not None and fdot is not None:
        p = 1 / f
        pdot = -fdot / (f ** 2)
    elif p is not None and pdot is not None:
        f = 1 / p
        fdot = -pdot / (p ** 2)
    else:
        raise ValueError("Either (f, fdot) or (p, pdot) must be provided")
    return f, fdot, p, pdot



def calculate_spin_with_error(f=None, fdot=None, p=None, pdot=None, f0_err=None, f1_err=None, p_err=None, pdot_err=None):
    """
    Compute spin parameters with optional Gaussian error propagation.
    
    Supports both scalars and NumPy arrays.

    Parameters:
    - f, fdot: Frequency and frequency derivative.
    - p, pdot: Period and period derivative.
    - f0_err, f1_err, p_err, pdot_err: Corresponding errors.

    Returns:
    - f, fdot, p, pdot (values)
    - f_err, fdot_err, p_err, pdot_err (errors, if applicable)
    """

    if f is not None and fdot is not None:
        f, fdot = np.asarray(f), np.asarray(fdot)  # Ensure NumPy arrays
        if f0_err is not None and f1_err is not None:
            f0_err, f1_err = np.asarray(f0_err), np.asarray(f1_err)
            f_u = uarray(f, f0_err)
            fdot_u = uarray(fdot, f1_err)

            p_u = 1 / f_u
            pdot_u = -fdot_u / (f_u ** 2)

            return nominal_values(p_u), nominal_values(pdot_u), std_devs(p_u), std_devs(pdot_u)

        else:
            p = 1 / f
            pdot = -fdot / (f ** 2)
            return p, pdot, None, None

    elif p is not None and pdot is not None:
        p, pdot = np.asarray(p), np.asarray(pdot)
        if p_err is not None and pdot_err is not None:
            p_err, pdot_err = np.asarray(p_err), np.asarray(pdot_err)
            p_u = uarray(p, p_err)
            pdot_u = uarray(pdot, pdot_err)

            f_u = 1 / p_u
            fdot_u = -pdot_u / (p_u ** 2)

            return nominal_values(f_u), nominal_values(fdot_u), std_devs(f_u), std_devs(fdot_u)

        else:
            f = 1 / p
            fdot = -pdot / (p ** 2)
            return f, fdot, None, None

    else:
        raise ValueError("Either (f, fdot) or (p, pdot) must be provided")



def pulsarx_to_kafka_message_format(filenames, pulsarx_cand_file, search_candidates_file, data_product_ids, fold_candidates_database_uuid_list, output_file):
    fold_data = pd.read_csv(pulsarx_cand_file, skiprows=11, sep='\s+')
    fold_data['fold_dp_output_uuid'] = data_product_ids
    fold_data['fold_candidates_database_uuid'] = fold_candidates_database_uuid_list
    fold_data['fold_cands_filename'] = filenames
    search_data = pd.read_csv(search_candidates_file)
    #index is matching key
    search_data['index'] = search_data.index + 1
    df = pd.merge(fold_data, search_data, left_on='#id', right_on='index', how='inner')
    del df['index']
   
    p, pdot, p_error, pdot_error = calculate_spin_with_error(f=df['f0_new'].values, fdot=df['f1_new'].values, f0_err=df['f0_err'].values, f1_err=df['f1_err'].values)

    df['p0_new'] = p
    df['p1_new'] = pdot
    df['p0_err'] = p_error
    df['p1_err'] = pdot_error
    f, fdot, p, pdot = calculate_spin(f=df['f0_old'].values, fdot=df['f1_old'].values)
    df['p0_old'] = p
    df['p1_old'] = pdot
    #There is no p0_old_err and p1_old_err in the search_candidates_file
    df = df.astype({
        "#id": int, "fold_cands_filename": str, "f0_new": float, "f1_new": float,
        "dm_new": float, "S/N_new": float, "f0_old": float, "f1_old": float,
        "dm_old": float, "S/N": float, "p0_old": float, "p1_old": float,
        "p0_new": float, "p1_new": float, "p0_err" : float, "p1_err" : float, "search_candidates_database_uuid": str
    })
    
    df.to_csv(output_file, index=False, float_format='%.18f')

def presto_kafka_message_format(pfd_files, xml_file, data_product_ids, fold_candidates_database_uuid_list, output_file):
    fold_data = []
    import presto.prepfold as pp 
    for filename in pfd_files:
        pfd_data = pp.pfd(filename)
        bestprof = f"{filename}.bestprof"
        basename = os.path.basename(filename)
        candidate_id = int(basename.split('_')[5])

        if not os.path.isfile(bestprof):
            cmds = f"show_pfd -fixchi -noxwin {filename}"
            subprocess.check_output(cmds, shell=True)
        
        try: 
            bestprof_data = parse_bestprof(bestprof)
            snr_new = bestprof_data['Sigma']
            dm_new = float(bestprof_data['Best DM'])
            p0_new = float(bestprof_data['P_topo (ms)']) / 1e3
            p1_new = float(bestprof_data["P'_topo (s/s)"]) 
            f0_new, f1_new, p0_new, p1_new = calculate_spin(p=p0_new, pdot=p1_new)
        except:
            snr_new = float(pfd_data.calc_sigma())
            dm_new = float(pfd_data.bestdm)
            try:
                p0_new = float(pfd_data.topo_p1)
                p1_new = float(pfd_data.topo_p2)
            except:
                p0_new = float(pfd_data.bary_p1)
                p1_new = float(pfd_data.bary_p2)
            f0_new, f1_new, p0_new, p1_new = calculate_spin(p=p0_new, pdot=p1_new)

        fold_data.append([candidate_id, filename, f0_new, f1_new, p0_new, p1_new, dm_new, snr_new])
    
    fold_data = pd.DataFrame(fold_data, columns=[
        '#id', 'fold_cands_filename', 'f0_new', 'f1_new', 'p0_new', 'p1_new',
        'dm_new', 'S/N_new'
    ])
    fold_data['fold_dp_output_uuid'] = data_product_ids
    fold_data['fold_candidates_database_uuid'] = fold_candidates_database_uuid_list
    search_data = get_xml_cands(xml_file)
    search_data['p1_old'] = a_to_pdot(search_data['period'].values, search_data['acc'].values)
    
    new_column_names = {
        'snr': 'S/N',
        'period': 'p0_old',
        'dm': 'dm_old'
    }
    search_data.rename(columns=new_column_names, inplace=True)
    f, fdot, p, pdot = calculate_spin(p=search_data['p0_old'].values, pdot=search_data['p1_old'].values)
    search_data['f0_old'] = f
    search_data['f1_old'] = fdot
    search_data['cand_id_in_file_match'] = search_data['cand_id_in_file'] + 1
    df = pd.merge(fold_data, search_data, left_on='#id', right_on='cand_id_in_file_match', how='inner')
    del df['cand_id_in_file_match']
    df.to_csv(output_file, index=False)

def main():
    parser = argparse.ArgumentParser(description="Process pulsar data")
    parser.add_argument("-p", "--process_name", choices=["pulsarx", "presto"], required=True, help="Name of the process to execute")
    parser.add_argument("-f", "--filenames", nargs='+', required=True, help="List of filenames to process")
    parser.add_argument("-c", "--pulsarx_cand_file", help="PulsarX candidate file")
    parser.add_argument("-x", "--filtered_search_csv", required=True, help="Search CSV candidates selected from XML for folding")
    parser.add_argument("-d", "--data_product_ids", nargs='+', required=True, help="Data product IDs")
    parser.add_argument("-u", "--fold_cands_database_uuid", nargs='+', required=True, help="Fold candidates database UUID")
    parser.add_argument("-o", "--output_file", help="Output file name", default="search_fold_merged.csv")

    args = parser.parse_args()

    if args.process_name == "pulsarx":
        logging.info("Processing with PulsarX format")
        pulsarx_to_kafka_message_format(
            args.filenames, args.pulsarx_cand_file, args.filtered_search_csv,
            args.data_product_ids, args.fold_cands_database_uuid, args.output_file
        )
    elif args.process_name == "presto":
        logging.info("Processing with Presto format")
        logging.info("Not implemented yet")
        sys.exit(1)

if __name__ == "__main__":
    main()
