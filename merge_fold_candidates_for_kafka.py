import argparse
import logging
import os
import pandas as pd
import xml.etree.ElementTree as ET
from bestprof_utils import parse_bestprof
import subprocess
import sys

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
        fdot = -pdot * (p ** 2)
    else:
        raise ValueError("Either (f, fdot) or (p, pdot) must be provided")
    return f, fdot, p, pdot

def get_xml_cands(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    header_params = root[1]
    search_params = root[2]
    candidates = root[7]

    filterbank_file = str(search_params.find("infilename").text)
    tsamp = float(header_params.find("tsamp").text)
    fft_size = int(search_params.find("size").text)
    nsamples = int(root.find("header_parameters/nsamples").text)
    tstart = float(header_params.find("tstart").text)
    source_name_prefix = str(header_params.find("source_name").text).strip()
    
    ignored_entries = ['candidate', 'opt_period', 'folded_snr', 'byte_offset', 'is_adjacent', 'is_physical']
    rows = []
    for candidate in candidates:
        cand_dict = {}
        for cand_entry in candidate.iter():
            if cand_entry.tag not in ignored_entries:
                cand_dict[cand_entry.tag] = cand_entry.text
        cand_dict['cand_id_in_file'] = candidate.attrib.get("id")
        rows.append(cand_dict)

    df = pd.DataFrame(rows)
    df = df.astype({
        "snr": float, "dm": float, "period": float, "nh": int, "acc": float,
        "nassoc": int, "ddm_count_ratio": float, "ddm_snr_ratio": float, "cand_id_in_file": int
    })

    return df

def pulsarx_to_kafka_message_format(filenames, pulsarx_cand_file, xml_file, data_product_ids, fold_candidates_database_uuid_list, output_file):
    fold_data = pd.read_csv(pulsarx_cand_file, skiprows=11, sep='\s+')
    fold_data['fold_dp_output_uuid'] = data_product_ids
    fold_data['fold_candidates_database_uuid'] = fold_candidates_database_uuid_list
    fold_data['fold_cands_filename'] = filenames
    
    search_data = get_xml_cands(xml_file)
 
    search_data['cand_id_in_file_match'] = search_data['cand_id_in_file'] + 1
    df = pd.merge(fold_data, search_data, left_on='#id', right_on='cand_id_in_file_match', how='inner')
    del df['cand_id_in_file_match']
    f, fdot, p, pdot = calculate_spin(f=df['f0_new'].values, fdot=df['f1_new'].values)
    df['p0_new'] = p
    df['p1_new'] = pdot
    f, fdot, p, pdot = calculate_spin(f=df['f0_old'].values, fdot=df['f1_old'].values)
    df['p0_old'] = p
    df['p1_old'] = pdot
    df = df.astype({
        "#id": int, "fold_cands_filename": str, "f0_new": float, "f1_new": float,
        "dm_new": float, "S/N_new": float, "f0_old": float, "f1_old": float,
        "dm_old": float, "S/N": float, "p0_old": float, "p1_old": float,
        "p0_new": float, "p1_new": float, "search_candidates_database_uuid": str
    })
    
    df.to_csv(output_file, index=False)

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
    parser.add_argument("-x", "--xml_file", required=True, help="XML file with candidate data")
    parser.add_argument("-d", "--data_product_ids", nargs='+', required=True, help="Data product IDs")
    parser.add_argument("-u", "--fold_cands_database_uuid", nargs='+', required=True, help="Fold candidates database UUID")
    parser.add_argument("-o", "--output_file", help="Output file name", default="search_fold_merged.csv")

    args = parser.parse_args()

    if args.process_name == "pulsarx":
        logging.info("Processing with PulsarX format")
        pulsarx_to_kafka_message_format(
            args.filenames, args.pulsarx_cand_file, args.xml_file,
            args.data_product_ids, args.fold_cands_database_uuid, args.output_file
        )
    elif args.process_name == "presto":
        logging.info("Processing with Presto format")
        presto_kafka_message_format(
            args.filenames, args.xml_file, args.data_product_ids,
            args.fold_cands_database_uuid, args.output_file
        )

if __name__ == "__main__":
    main()
