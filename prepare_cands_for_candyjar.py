import argparse
import glob
import astropy.units as u
from astropy.coordinates import Angle
from astropy.coordinates import SkyCoord
from astropy.time import Time
#from presto import chi2_sigma
import os, sys
import bestprof_utils
import pandas as pd
import ast
import subprocess
import json
from datetime import datetime
import xml.etree.ElementTree as ET
import pygedm
def pdot_acc_conversion(P_s, acc_ms2=None, pdot=None):
    LIGHT_SPEED = 2.99792458e8  # Speed of Light in SI

    if acc_ms2 is not None and pdot is not None:
        raise ValueError("Please provide either acc_ms2 or pdot, not both.")
    elif acc_ms2 is not None:
        # Convert acceleration to period derivative
        pdot = P_s * acc_ms2 / LIGHT_SPEED
        return pdot
    elif pdot is not None:
        # Convert period derivative to acceleration
        acc_ms2 = pdot * LIGHT_SPEED / P_s
        return acc_ms2
    else:
        raise ValueError("Please provide Period and either acc_ms2 or pdot.")


def initialize_configs(file_path):
    """
    Parses the config file and extracts all parameters from the nextflow configuration file.
    It strips the 'params.' prefix from keys if present and keeps all other keys unchanged.
    """
    nextflow_config = parse_nextflow_flat_config_from_file(file_path)
    # Create a new dictionary, adjusting keys to remove the 'params.' prefix where applicable,
    # but include all entries regardless of prefix
    params = {key.replace('params.', '') if key.startswith('params.') else key: value 
              for key, value in nextflow_config.items()}
    return params

def parse_nextflow_flat_config_from_file(file_path):
    config = {}
    with open(file_path, 'r') as file:
        for line in file:
            # Split each line by the first '=' to separate the key and value
            if "=" in line:
                key, value = line.split("=", 1)
                # Trim whitespace and remove surrounding quotes from the value if present
                key = key.strip()
                value = value.strip().strip("'\"")
                config[key] = value
    return config



def get_args():

    arg_parser = argparse.ArgumentParser(
        description="A utility tool to obtain a csv file from presto/pulsarx candidates to view with CandyJar"
    )
    arg_parser.add_argument("-d", "--data", help="Input Data Directory", required=True)
    arg_parser.add_argument("-m", "--meta", help="apsuse.meta file of that observation", required=True)
    arg_parser.add_argument("-c", "--config", help="Nextflow Config File used for processing", default='data_config.cfg')
    arg_parser.add_argument("-db", "--db_config", help="Nextflow JSON file used for processing", default='raw_dp_with_ids.json')
    arg_parser.add_argument("-b", "--bary", help="Get optimised barycentric values instead of topocentric", action="store_true")
    arg_parser.add_argument("-o", "--out", help="Output csv file", default="candidates.csv")
    arg_parser.add_argument("-u", "--utc", help="UTC of observation in ISOT format - default=start MJD")
    arg_parser.add_argument("-f", "--filterbank", default=None, help="Path to filterbank file")
    arg_parser.add_argument("-p", "--process", default="presto", help="Folding Program that created the file. Eg PulsarX/Presto")
    arg_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    arg_parser.add_argument("--copy_ml_cands_only", action="store_true", help="Copy only ML candidates")                                      
    return arg_parser.parse_args()

def get_galactic_from_equatorial(ra_hms, dec_dms):
    ra = Angle(ra_hms, unit=u.hour)
    dec = Angle(dec_dms, unit=u.degree)
    coord = SkyCoord(ra=ra, dec=dec, frame='icrs')
    return coord.galactic.l.deg, coord.galactic.b.deg

def get_galactic_from_equatorial_hms_dms(ra_hms, dec_dms):
    ra = Angle(ra_hms, unit=u.hourangle)
    dec = Angle(dec_dms, unit=u.deg)
    coord = SkyCoord(ra=ra, dec=dec, frame='icrs')
    return coord.galactic.l.deg, coord.galactic.b.deg

def get_isot_from_mjd(mjd):
    return Time(mjd, format='mjd', scale='utc').isot


def convert_to_float(value):
    result = ast.literal_eval(value)
    if isinstance(result, list) or isinstance(result, tuple):
        return float(result[0])
    return float(result)


def calculate_spin(f=None, fdot=None, p=None, pdot=None):
        # calculate p and pdot from f and fdot
        if f is not None and fdot is not None:
            p = 1 / f
            pdot = -fdot / (f**2)
        # calculate f and fdot from p and pdot
        elif p is not None and pdot is not None:
            f = 1 / p
            fdot = -pdot * (p**2)
        else:
            raise ValueError("Either (f, fdot) or (p, pdot) must be provided")
            
        return f, fdot, p, pdot


def create_candyjar_csv_presto(search_fold_merged, metafile, filterbank_file, candyjar_output_file, pointing_id, beam_id, beam_name, source_name, utc_start, header):
    """
    Create a csv file to be used with CandyJar from the search_fold_merged.csv file
    """
    
    import presto.prepfold as pp
    #Each file is assumed to be a beam, hence same ra and dec
    first_file = search_fold_merged['fold_cands_filename'].iloc[0]
    sample_pfd = f"{data_dir}/{first_file}"
    ra = pp.pfd(sample_pfd).rastr.decode('utf-8')
    dec = pp.pfd(sample_pfd).decstr.decode('utf-8')
  
    gl, gb = get_galactic_from_equatorial_hms_dms(ra, dec)

    f0_opt_err = 0.0
    f1_opt_err = 0.0
    acc_opt_err = 0.0
    dm_opt_err = 0.0
    pics_palfa = 0.0
    #ML scores will be added later
    pics_trapum_ter5 = 0.0
    pics_m_LS_recall = 0.0
    pics_pm_LS_fscore = 0.0
    maxdm_ymw16, tau_sc = pygedm.dist_to_dm(gl, gb, 50000, method='ymw16')
    maxdm_ymw16 = maxdm_ymw16.value
    filterbank_path = os.path.abspath(filterbank_file)
    with open(candyjar_output_file, 'w') as out:
        out.write(header + "\n")
        for index, row in search_fold_merged.iterrows():
            
            filename = row['fold_cands_filename']
            f0_user = row['f0_old']
            f1_user = row['f1_old']
            f0_opt = row['f0_new']
            f1_opt = row['f1_new']
            acc_user = row['acc']
            acc_opt = pdot_acc_conversion(row['p0_new'], pdot = row['p1_new'])
            dm_user = row['dm_old']
            dm_opt = row['dm_new']
            sn_fft = row['S/N']
            sn_fold = row['S/N_new']
            pfd_obj = pp.pfd(f"{data_dir}/{filename}")
            pepoch = pfd_obj.tepoch
            dist_ymw16, tau_sc = pygedm.dm_to_dist(gl, gb, dm_opt, method='ymw16')
            dist_ymw16 = dist_ymw16.value
            png_path = filename + '.png'
            mjd_start = pfd_obj.tepoch
            metafile_path = metafile
            candidate_tarball_path = None
            if args.copy_ml_cands_only:
                if pics_m_LS_recall < 0.1 and pics_pm_LS_fscore < 0.1:
                    continue
            
            out.write("{},".format(pointing_id))
            out.write("{:d},".format(beam_id))
            out.write("{},".format(beam_name))
            out.write("{},".format(source_name))
            out.write("{},".format(ra))
            out.write("{},".format(dec))
            out.write("{},".format(gl))
            out.write("{},".format(gb))
            out.write("{:15.10f},".format(mjd_start))
            out.write("{},".format(utc_start))
            out.write("{:13.9f},".format(f0_user))
            out.write("{:13.9f},".format(f0_opt))
            out.write("{:13.9f},".format(f0_opt_err))
            out.write("{:13.9f},".format(f1_user))
            out.write("{:13.9f},".format(f1_opt))
            out.write("{:13.9f},".format(f1_opt_err))
            out.write("{:13.9f},".format(acc_user))
            out.write("{:13.9f},".format(acc_opt))
            out.write("{:13.9f},".format(acc_opt_err))
            out.write("{:13.9f},".format(dm_user))
            out.write("{:13.9f},".format(dm_opt))
            out.write("{:13.9f},".format(dm_opt_err))
            out.write("{:13.9f},".format(sn_fft))
            out.write("{:13.9f},".format(sn_fold))
            out.write("{:15.10f},".format(pepoch))
            out.write("{:13.9f},".format(maxdm_ymw16))
            out.write("{:13.9f},".format(dist_ymw16))
            out.write("{:f},".format(pics_trapum_ter5))
            out.write("{:f},".format(pics_palfa))
            out.write("{:f},".format(pics_m_LS_recall))
            out.write("{:f},".format(pics_pm_LS_fscore))
            out.write("{},".format(png_path))
            out.write("{},".format(metafile_path))
            out.write("{},".format(filterbank_path))
            out.write("{}\n".format(candidate_tarball_path))


def create_candyjar_csv_pulsarx(search_fold_merged, metafile, filterbank_file, candyjar_output_file, pointing_id, beam_id, beam_name, source_name, utc_start, header, pulsarx_metadata):
    """
    Create a csv file to be used with CandyJar from the search_fold_merged.csv file
    """
    #Each file is assumed to be a beam, hence same ra and dec
   
    ra = pulsarx_metadata['RA']
    dec = pulsarx_metadata['DEC']
    gl = float(pulsarx_metadata['GL'])
    gb = float(pulsarx_metadata['GB'])
    mjd_start = float(pulsarx_metadata['Date'])
    pepoch = float(pulsarx_metadata['Pepoch'])
    maxdm_ymw16 = float(pulsarx_metadata['MaxDM_YMW16'])
    filterbank_path = pulsarx_metadata['Filename']
    f0_opt_err = 0.0
    f1_opt_err = 0.0
    acc_opt_err = 0.0
    dm_opt_err = 0.0
    #ML scores will be added later
    pics_palfa = 0.0
    pics_trapum_ter5 = 0.0
    pics_m_LS_recall = 0.0
    pics_pm_LS_fscore = 0.0
 
    with open(candyjar_output_file, 'w') as out:
        out.write(header + "\n")
        for index, row in search_fold_merged.iterrows():           
            filename = row['fold_cands_filename']
            f0_user = row['f0_old']
            f1_user = row['f1_old']
            f0_opt = row['f0_new']
            f1_opt = row['f1_new']
            acc_user = row['acc']
            acc_opt = row['acc_new']
            dm_user = row['dm']
            dm_opt = row['dm_new']
            sn_fft = row['snr']
            sn_fold = row['S/N_new']
            dist_ymw16, tau_sc = pygedm.dm_to_dist(gl, gb, dm_opt, method='ymw16')
            dist_ymw16 = dist_ymw16.value
            #Remove last extension and add .png
            png_path = os.path.splitext(filename)[0] + '.png'
            candidate_tarball_path = None
            if args.copy_ml_cands_only:
                if pics_m_LS_recall < 0.1 and pics_pm_LS_fscore < 0.1:
                    continue
                    
            out.write("{},".format(pointing_id))
            out.write("{:d},".format(beam_id))
            out.write("{},".format(beam_name))
            out.write("{},".format(source_name))
            out.write("{},".format(ra))
            out.write("{},".format(dec))
            out.write("{},".format(gl))
            out.write("{},".format(gb))
            out.write("{:15.10f},".format(mjd_start))
            out.write("{},".format(utc_start))
            out.write("{:13.9f},".format(f0_user))
            out.write("{:13.9f},".format(f0_opt))
            out.write("{:13.9f},".format(f0_opt_err))
            out.write("{:13.9f},".format(f1_user))
            out.write("{:13.9f},".format(f1_opt))
            out.write("{:13.9f},".format(f1_opt_err))
            out.write("{:13.9f},".format(acc_user))
            out.write("{:13.9f},".format(acc_opt))
            out.write("{:13.9f},".format(acc_opt_err))
            out.write("{:13.9f},".format(dm_user))
            out.write("{:13.9f},".format(dm_opt))
            out.write("{:13.9f},".format(dm_opt_err))
            out.write("{:13.9f},".format(sn_fft))
            out.write("{:13.9f},".format(sn_fold))
            out.write("{:15.10f},".format(pepoch))
            out.write("{:13.9f},".format(maxdm_ymw16))
            out.write("{:13.9f},".format(dist_ymw16))
            out.write("{:f},".format(pics_trapum_ter5))
            out.write("{:f},".format(pics_palfa))
            out.write("{:f},".format(pics_m_LS_recall))
            out.write("{:f},".format(pics_pm_LS_fscore))
            out.write("{},".format(png_path))
            out.write("{},".format(metafile))
            out.write("{},".format(filterbank_path))
            out.write("{}\n".format(candidate_tarball_path))




           
            

if __name__ == '__main__':
    header = "pointing_id,beam_id,beam_name,source_name,ra,dec,gl,gb,mjd_start,utc_start,f0_user,f0_opt,f0_opt_err,f1_user,f1_opt,f1_opt_err,acc_user,acc_opt,acc_opt_err,dm_user,dm_opt,dm_opt_err,sn_fft,sn_fold,pepoch,maxdm_ymw16,dist_ymw16,pics_trapum_ter5,pics_palfa,pics_meerkat_l_sband_combined_best_recall,pics_palfa_meerkat_l_sband_best_fscore,png_path,metafile_path,filterbank_path,candidate_tarball_path"
    args = get_args()
    #args.config is created frm running the command below combining all nextflow config files
    #nextflow config -profile nt -flat -sort > data_config.cfg

    data_dir = args.data
    search_fold_merged = f"{data_dir}/search_fold_merged.csv"
    
    if not os.path.isfile(search_fold_merged):
        print(f"search_fold_merged.csv not found in {data_dir}")
        sys.exit()
    
    df = pd.read_csv(search_fold_merged)
 
    pics_results = f"{data_dir}/pics_scores.csv"
    if os.path.isfile(pics_results):
        pics_scores = pd.read_csv(pics_results)
    else:
        pics_palfa, pics_trapum_ter5, pics_m_LS_recall, pics_pm_LS_fscore = 0, 0, 0, 0
    
    if args.out:
        candyjar_output_file = f"{data_dir}/{args.out}"
    else:
        candyjar_output_file = f"{data_dir}/candidates.csv"
    
    #Read the nextflow json file
    with open(args.db_config) as f:
        db_config = json.load(f)
    
    
    pipeline_config = initialize_configs(args.config)
    pointing_id = db_config['pointing_id']
    beam_id = db_config['beam_id']

    beam_name = pipeline_config['beam']
    source_name = pipeline_config['target']

    if args.utc:
        utc_start = args.utc
    else:
        utc_start = pipeline_config['utc']
        utc_obj = datetime.strptime(utc_start, "%Y-%m-%d-%H:%M:%S")
        utc_start = utc_obj.isoformat()

    if beam_name == "ptuse":

        if args.process == "presto":

            create_candyjar_csv_presto(df, args.meta, args.filterbank, candyjar_output_file, pointing_id, beam_id, beam_name, source_name, utc_start, header)

        elif args.process == "pulsarx":

            pulsarx_cand_file = glob.glob(f"{data_dir}/*.cands")
            # Read the first 10 rows of the file
            pulsarx_cand_file = pd.read_csv(pulsarx_cand_file[0], nrows=11, header=None)

            # Process the file to remove hashes and split into key-value pairs
            pulsarx_cand_file['line'] = pulsarx_cand_file[0].str.lstrip('#')
            pulsarx_cand_file[['key', 'value']] = pulsarx_cand_file['line'].str.split(n=1, expand=True)

            # Convert to dictionary
            pulsarx_metadata = pd.Series(pulsarx_cand_file['value'].values, index=pulsarx_cand_file['key']).to_dict()
            create_candyjar_csv_pulsarx(df, args.meta, args.filterbank, candyjar_output_file, pointing_id, beam_id, beam_name, source_name, utc_start, header, pulsarx_metadata)
    