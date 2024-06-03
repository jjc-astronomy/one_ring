#from prepfold import pfd
import presto.prepfold as pp
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
    arg_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    # arg_parser = argparse.ArgumentParser(
    #     description="A utility tool to obtain a csv file from presto/pulsarx candidates to view with CandyJar ")
    # arg_parser.add_argument("-data", dest="data_dir", help="Input Data Directory", required=True)
    # arg_parser.add_argument("-meta", dest="meta_path", help="apsuse.meta file of that observation", required=True)
    # arg_parser.add_argument("-config", dest="nextflow_config", help="Nextflow Config File used for processing", default='data_config.cfg')
    # arg_parser.add_argument("-db_config", dest="nextflow_db_config", help="Nextflow JSON file used for processing", default='raw_dp_with_ids.json')
    # #arg_parser.add_argument("-xml_file", dest="peasoup_xml_file", help="Peasoup XML File", required=True)
    # arg_parser.add_argument("-bary", dest="bary", help="Get optimised barycentric values instead of topocentric", action="store_true")
    # arg_parser.add_argument("-out", dest="outfile", help="Output csv file", default="candidates.csv") 
    # arg_parser.add_argument("-utc", dest="utc", help="UTC of observation in ISOT format - default=start MJD")      
    # arg_parser.add_argument("-filterbank_path", dest="filterbank_path",default=None,help="Path to filterbank file")
    # arg_parser.add_argument("-verbose", dest="verbose",action="store_true",help="Verbose output")
    # #arg_parser.add_argument('-copy_ml_cands_only', action='store_true', default=False, help='Copy high scoring ML candidates only')


                                                     
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


def p_to_f(p, pd, pdd=None):
    """
    p_to_f(p, pd, pdd=None):
       Convert period, period derivative and period second
       derivative to the equivalent frequency counterparts.
       Will also convert from f to p.
    """
    f = 1.0 / p
    fd = -pd / (p * p)
    if (pdd is None):
        return [f, fd]
    else:
        if (pdd == 0.0):
            fdd = 0.0
        else:
            fdd = 2.0 * pd * pd / (p ** 3.0) - pdd / (p * p)
        return [f, fd, fdd]

def create_candyjar_csv_presto(search_fold_merged, pfd_files, metafile, filterbank_file, candyjar_output_file, pointing_id, beam_id, beam_name, source_name, utc_start):
    header = "pointing_id,beam_id,beam_name,source_name,ra,dec,gl,gb,mjd_start,utc_start,f0_user,f0_opt,f0_opt_err,f1_user,f1_opt,f1_opt_err,acc_user,acc_opt,acc_opt_err,dm_user,dm_opt,dm_opt_err,sn_fft,sn_fold,pepoch,maxdm_ymw16,dist_ymw16,pics_trapum_ter5,pics_palfa,pics_meerkat_l_sband_combined_best_recall,pics_palfa_meerkat_l_sband_best_fscore,png_path,metafile_path,filterbank_path,candidate_tarball_path"
    ra = pp.pfd(pfd_files[0]).rastr.decode('utf-8')
    dec = pp.pfd(pfd_files[0]).decstr.decode('utf-8')
  
    gl, gb = get_galactic_from_equatorial_hms_dms(ra, dec)

    f0_opt_err = 0.0
    f1_opt_err = 0.0
    acc_opt_err = 0.0
    pics_palfa = 0.0
    pics_trapum_ter5 = 0.0
    pics_m_LS_recall = 0.0
    pics_pm_LS_fscore = 0.0

    

    with open(candyjar_output_file, 'w') as out:
        out.write(header + "\n")
        for index, row in search_fold_merged.iterrows():
            filename = row['fold_cands_filename']
            png_filename = filename + '.png'
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


            print(acc_user, acc_opt)
            sys.exit()


        for f in pfd_files:
            basename = os.path.basename(f)
            
            bestprof = bestprof_utils.parse_bestprof(f + ".bestprof")
            pfd_file = pp.pfd(f)
            mjd_start = pfd_file.tepoch
            if utc_start is None:
                utc_start = get_isot_from_mjd(mjd_start)
            
            f0_user = pfd_file.fold_p1
            f1_user = pfd_file.fold_p2
            f2_user = pfd_file.fold_p3
            acc_user = f1_user * 2.99792458e8 / f0_user

            if args.bary:
                f0_opt, f1_opt, f2_opt = p_to_f(pfd_file.bary_p1, pfd_file.bary_p3, pfd_file.bary_p3)
            else:
                f0_opt, f1_opt, f2_opt = p_to_f(pfd_file.topo_p1, pfd_file.topo_p2, pfd_file.topo_p3)
            
            acc_opt =  f1_opt * 2.99792458e8 / f0_opt
            dm_opt = pfd_file.bestdm
           
            



            # if args.copy_ml_cands_only:
            #     if pics_m_LS_recall < 0.1 and pics_pm_LS_fscore < 0.1:
            #         continue
            # bestprof = bestprof_utils.parse_bestprof(f + ".bestprof")
            # try:
            #     pfd_file = pfd(f)
            # except:
            #     print("Error reading pfd file {}".format(f))
            #     continue
            # basename = os.path.basename(f)
            # if args.search_files_dir:
            #     search_dir = args.search_files_dir
            #     trimmed_filename = basename.replace(".pfd", "")
            #     trimmed_filename = trimmed_filename.split("_")[2:]
            #     print(trimmed_filename)
            #     sys.exit()
            # else:
            #     sn_fft = 0
            # beam_name = args.beam_name
            # source_name = pfd_file.candnm
            


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
    print(df)
    print(df.columns)
   
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
        pfd_files = glob.glob(f"{data_dir}/*.pfd")
        archive_files = glob.glob(f"{data_dir}/*.ar")
        if len(pfd_files) > 0:
            print(f"Found {len(pfd_files)} pfd files in {data_dir}")
            create_candyjar_csv_presto(df, pfd_files, args.meta, args.filterbank, candyjar_output_file, pointing_id, beam_id, beam_name, source_name, utc_start)
        
    #with open(candyjar_output_file, 'w') as out:

    # pfd_files = glob.glob(candidate_dir + '/' + '*.pfd')
    # current_directory = os.getcwd()
    # code_dir = args.code_dir
    # output_meta_dir = code_dir + '/' + cluster + '/' + epoch + '/' 
  
    # #search_dir = args.search_files
   
    
   
    # #output_meta_dir =  current_directory + '/' + 'CANDIDATE_VIEWER/' + cluster + '/' + epoch + '/'  + 'ML_SELECTED/' + 'metafiles' + '/' 
    # candidates_csv_output = candidate_dir + '/' + 'candidates.csv'


    # # Copy meta file if not already there
    # if not os.path.exists(output_meta_dir + '/' + os.path.basename(args.meta_path)):
    #     os.system("cp {} {}".format(args.meta_path, output_meta_dir))
     
    # if args.verbose:
    #     print(pfd_files)
    # print("{} pfd files for beam {}".format(len(pfd_files), args.beam_name))
    # # Check if the file exists
    # #file_exists = os.path.exists(candidates_csv_output)

    # # Open the file in append mode if it exists, else open in write mode
    # with open(candidates_csv_output, 'w') as out:
        
    #     out.write(header + "\n")

    #     for index, row in pics_scores.iterrows():
    #         filename = row['filename']
    #         png_filename = filename + '.png'
    #         pics_palfa = row['clfl2_PALFA']
    #         pics_trapum_ter5 = row['clfl2_trapum_Ter5']
    #         pics_m_LS_recall = row['MeerKAT_L_SBAND_COMBINED_Best_Recall']
    #         pics_pm_LS_fscore = row['PALFA_MeerKAT_L_SBAND_Best_Fscore']
    #         pfd_file = candidate_dir + '/' + row['filename']
    #         try:
    #             pfd_obj = pfd(pfd_file)
    #         except:
    #             print("Error reading pfd file {}".format(pfd_file))
    #             continue

    #         if os.path.isfile(pfd_file + ".bestprof"):
    #             bestprof = bestprof_utils.parse_bestprof(pfd_file + ".bestprof")
            
    #         else:
    #             os.chdir(candidate_dir)
    #             cmds = "show_pfd -noxwin -fixchi %s" % row['filename']
    #             subprocess.check_output(cmds, shell=True)
    #             bestprof = bestprof_utils.parse_bestprof(pfd_file + ".bestprof")
    #             #os.chdir(current_directory)


    #         if args.search_files_dir:
    #             search_dir = args.search_files_dir
    #             trimmed_filename = os.path.splitext(filename)[0]
    #             segment = trimmed_filename.split("_")[5]
    #             segment = segment.replace('m', '')
    #             chunk = trimmed_filename.split("_")[6]
    #             cand_number = trimmed_filename.split("_")[-1]
    #             dm_value = trimmed_filename.split("_")[7]
    #             if "ACCEL" in trimmed_filename:   
    #                 zmax = trimmed_filename.split("_")[8].replace('z', '')
                
    #             else:
    #                 #JERK CANDIDATE
    #                 zmax = trimmed_filename.split("_")[8].replace('z', '') 
    #                 wmax = trimmed_filename.split("_")[9].replace('w', '')

              
    #             #Read the search file
    #             if segment != 'full':
    #                 file_location = search_dir + '/' + cluster + '_' + epoch + '_' + beam + '/' + segment + 'm' + '/' + chunk + '/'
                    
    #             else:
    #                 file_location = search_dir + '/' + cluster + '_' + epoch + '_' + beam + '/' + segment + '/' + chunk + '/'

    #             if "ACCEL" in trimmed_filename:
    #                 search_filename = file_location + cluster + '_' + epoch + '_' + beam + '_' + segment + '_' + chunk + '_' + dm_value + '_ACCEL_' + zmax
    #             else:
    #                 search_filename = file_location + cluster + '_' + epoch + '_' + beam + '_' + segment + '_' + chunk + '_' + dm_value + '_ACCEL_' + zmax + '_JERK_' + wmax 

    #             #fourier_detections = fourierprops()
    #             #get_rzw_cand(search_filename + '.cand', int(cand_number), fourier_detections)
    #             #moving epoch of r,z, w to start of file for fair comparison
    #             #z0 = fourier_detections.z - 0.5 * fourier_detections.w
    #             #r0 = fourier_detections.r - 0.5 * z0 - fourier_detections.w / 6.0
    #             #f2_user = 0.0
    #             #sn_fft = fourier_detections.sig

    #             # New additions from updated parser presto_sifter.py
    #             f0_user, f1_user, f2_user, acc_user, sn_fft = parse_accel_search_cand_list(search_filename, int(cand_number))
    #             observation_time = float(bestprof['T_sample']) * int(bestprof['Data Folded'])
    #             sn_fold = float(bestprof['Sigma'])
    #             ra = pfd_obj.rastr
    #             dec = pfd_obj.decstr
    #             gl, gb = get_galactic_from_equatorial(ra, dec)
    #             mjd_start = pfd_obj.tepoch
    #             if (args.utc is None):
    #                 utc_start = get_isot_from_mjd(mjd_start)
    #             else:
    #                 utc_start = args.utc
                
    #             if (args.bary):
    #                 f0_opt, f1_opt, f2_opt = p_to_f(
    #                     pfd_obj.bary_p1, pfd_obj.bary_p2, pfd_obj.bary_p3)
    #             else:
    #                 f0_opt, f1_opt, f2_opt = p_to_f(
    #                     pfd_obj.topo_p1, pfd_obj.topo_p2, pfd_obj.topo_p3)
                
              
    #             f0_opt_err = 0.0
    #             f1_opt_err = 0.0
    #             f2_opt_err = 0.0
    #             acc_opt =  f1_opt * 2.99792458e8 / f0_opt
    #             acc_opt_err = 0.0
    #             dm_user = float(dm_value.replace('DM', ''))
    #             dm_opt = pfd_obj.bestdm
    #             pepoch = mjd_start
    #             maxdm_ymw16 = 0
    #             dist_ymw16 = 0
    #             #maxdm_ymw16 = pygedm.dist_to_dm(gl, gb, 50000, method='ymw16')
    #             #dist_ymw16 = pygedm.dm_to_dist(gl, gb, dm_opt, method='ymw16')
    #             pointing_id = beam_id = 0
    #             png_path = pfd_file + ".png"
    #             metafile_path = args.meta_path
    #             filterbank_path = args.filterbank_path
    #             candidate_tarball_path = None
    #             out.write("{},".format(pointing_id))
    #             out.write("{:d},".format(beam_id))
    #             out.write("{},".format(beam))
    #             source_name = cluster
    #             out.write("{},".format(source_name))
    #             out.write("{},".format(ra))
    #             out.write("{},".format(dec))
    #             out.write("{},".format(gl))
    #             out.write("{},".format(gb))
    #             out.write("{:15.10f},".format(mjd_start))
    #             out.write("{},".format(utc_start.split(".")[0]))
    #             out.write("{:13.9f},".format(f0_user))
    #             out.write("{:13.9f},".format(f0_opt))
    #             out.write("{:13.9f},".format(f0_opt_err))
    #             out.write("{:13.9f},".format(f1_user))
    #             out.write("{:13.9f},".format(f1_opt))
    #             out.write("{:13.9f},".format(f1_opt_err))
    #             # out.write("{:13.9f},".format(f2_user))
    #             # out.write("{:13.9f},".format(f2_opt))
    #             # out.write("{:13.9f},".format(f2_opt_err))
    #             out.write("{:13.9f},".format(acc_user))
    #             out.write("{:13.9f},".format(acc_opt))
    #             out.write("{:13.9f},".format(acc_opt_err))
    #             out.write("{:13.9f},".format(dm_user))
    #             out.write("{:13.9f},".format(dm_opt))
    #             dm_opt_err = 0.0
    #             out.write("{:13.9f},".format(dm_opt_err))
    #             out.write("{:13.9f},".format(sn_fft))
    #             out.write("{:13.9f},".format(sn_fold))
    #             out.write("{:15.10f},".format(pepoch))
    #             out.write("{:13.9f},".format(maxdm_ymw16))
    #             out.write("{:13.9f},".format(dist_ymw16))
    #             out.write("{:f},".format(pics_trapum_ter5))
    #             out.write("{:f},".format(pics_palfa))
    #             out.write("{:f},".format(pics_m_LS_recall))
    #             out.write("{:f},".format(pics_pm_LS_fscore))
    #             out.write("{},".format(png_filename))
    #             out.write("{},".format(metafile_path))
    #             out.write("{},".format(filterbank_path))
    #             out.write("{}".format(candidate_tarball_path))
    #             if args.verbose:
    #                 print(pointing_id, beam_id, beam, source_name, ra, dec, gl, gb, mjd_start, utc_start, f0_user, f0_opt, f0_opt_err, f1_user, f1_opt, f1_opt_err, f2_user, f2_opt, f2_opt_err, acc_user, acc_opt, acc_opt_err, dm_user, dm_opt, dm_opt_err, sn_fft, sn_fold, pepoch, maxdm_ymw16, dist_ymw16, pics_trapum_ter5, pics_palfa, png_path, metafile_path, filterbank_path, candidate_tarball_path)
               
    #             out.write("\n")
    #             out.flush()
                
    #             # Copy png file to output directory
    #             #os.system("cp {} {}".format(png_path, output_dir))
    #     print(beam, "Done.")
    #     df = pd.read_csv(candidates_csv_output)
    #     #Select high scoring ML candidates
    #     df1 = df.loc[(df['pics_meerkat_l_sband_combined_best_recall'] > 0.1) | (df['pics_palfa_meerkat_l_sband_best_fscore'] > 0.1)]
    #     df1.to_csv(candidate_dir + '/' + 'candidates_ml_selected.csv', index=False)



            

          


        # for f in args.pfds:
        #     basename = os.path.basename(f)
        #     print(pics_scores[pics_scores['filename'] == basename])
        #     sys.exit()
        #     pics_palfa = pics_scores[pics_scores['filename'] == basename]['clfl2_PALFA'].values[0]
        #     pics_trapum_ter5 = pics_scores[pics_scores['filename'] == basename]['clfl2_trapum_Ter5'].values[0]
        #     pics_m_LS_recall = pics_scores[pics_scores['filename'] == basename]['MeerKAT_L_SBAND_COMBINED_Best_Recall'].values[0]
        #     pics_pm_LS_fscore = pics_scores[pics_scores['filename'] == basename]['PALFA_MeerKAT_L_SBAND_Best_Fscore'].values[0]
        #     if args.copy_ml_cands_only:
        #         if pics_m_LS_recall < 0.1 and pics_pm_LS_fscore < 0.1:
        #             continue
            
            
        #     bestprof = bestprof_utils.parse_bestprof(f + ".bestprof")
        #     try:
        #         pfd_file = pfd(f)
        #     except:
        #         print("Error reading pfd file {}".format(f))
        #         continue
        #     basename = os.path.basename(f)
        #     if args.search_files_dir:
        #         search_dir = args.search_files_dir
        #         trimmed_filename = basename.replace(".pfd", "")
        #         trimmed_filename = trimmed_filename.split("_")[2:]
        #         print(trimmed_filename)
        #         sys.exit()
                
        #     else:
        #         sn_fft = 0


        #     beam_name = args.beam_name
        #     source_name = pfd_file.candnm
        #     ra = pfd_file.rastr
        #     dec = pfd_file.decstr
        #     gl, gb = get_galactic_from_equatorial(ra, dec)
        #     mjd_start = pfd_file.tepoch
        #     if (args.utc is None):
        #         utc_start = get_isot_from_mjd(mjd_start)
        #     else:
        #         utc_start = args.utc
        #     f0_user, f1_user, f2_user = p_to_f(
        #         pfd_file.curr_p1, pfd_file.curr_p2, pfd_file.curr_p3)
        #     acc_user = f1_user * 2.99792458e8 / f0_user

        #     if (args.bary):
        #         f0_opt, f1_opt, f2_opt = p_to_f(
        #             pfd_file.bary_p1, pfd_file.bary_p3, pfd_file.bary_p3)
        #     else:
        #         f0_opt, f1_opt, f2_opt = p_to_f(
        #             pfd_file.topo_p1, pfd_file.topo_p2, pfd_file.topo_p3)
        #     f0_opt_err = 0.0
        #     f1_opt_err = 0.0
        #     f2_opt_err = 0.0
        #     acc_opt =  f1_opt * 2.99792458e8 / f0_opt
        #     acc_opt_err = 0.0
        #     dm_user = pfd_file.bestdm
        #     dm_opt = pfd_file.bestdm
        #     dm_opt_err = 0.0
        #     sn_fft = 0

        #     sn_fold = float(bestprof['Sigma'])
        #     # sn_fold = pfd_file.chi2_sigma(pfd_file.calc_redchi2() *
        #     #                     pfd_file.DOFcor, int(pfd_file.DOFcor))

        #     pepoch = mjd_start
        #     maxdm_ymw16 = 0
        #     dist_ymw16 = 0
        #     pics_trapum_ter5 = 0
        #     pics_palfa = 0
        #     png_path = f + ".png"
        #     metafile_path = args.meta_path
        #     filterbank_path = args.filterbank_path
        #     candidate_tarball_path = None
        #     out.write("{},".format(pointing_id))
        #     out.write("{:d},".format(beam_id))
        #     out.write("{},".format(beam_name))
        #     out.write("{},".format(source_name))
        #     out.write("{},".format(ra))
        #     out.write("{},".format(dec))
        #     out.write("{},".format(gl))
        #     out.write("{},".format(gb))
        #     out.write("{:15.10f},".format(mjd_start))
        #     out.write("{},".format(utc_start.split(".")[0]))
        #     out.write("{:13.9f},".format(f0_user))
        #     out.write("{:13.9f},".format(f0_opt))
        #     out.write("{:13.9f},".format(f0_opt_err))
        #     out.write("{:13.9f},".format(f1_user))
        #     out.write("{:13.9f},".format(f1_opt))
        #     out.write("{:13.9f},".format(f1_opt_err))
        #     # out.write("{:13.9f},".format(f2_user))
        #     # out.write("{:13.9f},".format(f2_opt))
        #     # out.write("{:13.9f},".format(f2_opt_err))
        #     out.write("{:13.9f},".format(acc_user))
        #     out.write("{:13.9f},".format(acc_opt))
        #     out.write("{:13.9f},".format(acc_opt_err))
        #     out.write("{:13.9f},".format(dm_user))
        #     out.write("{:13.9f},".format(dm_opt))
        #     out.write("{:13.9f},".format(dm_opt_err))
        #     out.write("{:13.9f},".format(sn_fft))
        #     out.write("{:13.9f},".format(sn_fold))
        #     out.write("{:15.10f},".format(pepoch))
        #     out.write("{:13.9f},".format(maxdm_ymw16))
        #     out.write("{:13.9f},".format(dist_ymw16))
        #     out.write("{:f},".format(pics_trapum_ter5))
        #     out.write("{:f},".format(pics_palfa))
        #     out.write("{:f},".format(pics_m_LS_recall))
        #     out.write("{:f},".format(pics_pm_LS_fscore))
        #     #beam_num = beam_name.replace("cfbf00", "")
        #     out.write("{},".format('plots'  + "/" +  os.path.basename(png_path)))
        #     out.write("{},".format(metafile_path))
        #     out.write("{},".format(filterbank_path))
        #     out.write("{}".format(candidate_tarball_path))
        #     if args.verbose:
        #         print(pointing_id, beam_id, beam_name, source_name, ra, dec, gl, gb, mjd_start, utc_start, f0_user, f1_user, f2_user, acc_user, f0_opt, f1_opt, f2_opt, f0_opt, f1_opt, f2_opt, f0_opt_err, f1_opt_err,
        #       f2_opt_err, acc_opt, acc_opt_err, dm_user, dm_opt, dm_opt_err, sn_fft, sn_fold, pepoch, maxdm_ymw16, dist_ymw16, pics_trapum_ter5, pics_palfa, png_path, metafile_path, filterbank_path, candidate_tarball_path)
        #     out.write("\n")
        #     out.flush()

        #     # Copy png file to output directory
        #     os.system("cp {} {}".format(png_path, output_dir))
        # print(beam_name, "Done.")


