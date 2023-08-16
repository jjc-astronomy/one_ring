import xml.etree.ElementTree as ET
import sys, os, subprocess
import argparse
import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count



def generate_pulsarX_cand_file(cand_mod_frequencies, cand_dms, cand_accs, cand_snrs):

    cand_file_path = 'pulsarx.candfile' 
    #source_name_prefix = "%s_%s" % (beam_name, utc_name)
    with open(cand_file_path, 'w') as f:
        f.write("#id DM accel F0 F1 S/N\n")
        for i in range(len(cand_mod_frequencies)):
            f.write("%d %f %f %f 0 %f\n" % (i, cand_dms[i], cand_accs[i], cand_mod_frequencies[i], cand_snrs[i]))

    return cand_file_path

def period_correction_for_pulsarx(p0, pdot, no_of_samples, tsamp, fft_size):
    if (fft_size == 0.0):
        return p0 - pdot * \
            float(1 << (no_of_samples.bit_length() - 1) - no_of_samples) * tsamp / 2
    else:
        return p0 - pdot * float(fft_size - no_of_samples) * tsamp / 2

def period_correction_for_prepfold(p0,pdot,tsamp,fft_size):
    return p0 - pdot*float(fft_size)*tsamp/2

def a_to_pdot(P_s, acc_ms2):
    LIGHT_SPEED = 2.99792458e8                 # Speed of Light in SI
    return P_s * acc_ms2 /LIGHT_SPEED



def run_prepfold(args):
    row, filterbank_file, tsamp, fft_size, source_name_prefix, rfifind_mask = args
    peasoup_period = row['period']
    peasoup_acceleration = row['acc']
    pdot = a_to_pdot(peasoup_period, peasoup_acceleration)  
    fold_period = period_correction_for_prepfold(peasoup_period, pdot, tsamp, fft_size)  
    
    output_filename = source_name_prefix + '_Peasoup_fold_candidate_id_' + str(int(row['cand_id_in_file']) + 1) # Start indexing from 1 to match pulsarX
    dm = row['dm']

    if rfifind_mask is not None:
        cmd = "prepfold -fixchi -noxwin -topo -n 64 -mask %s -p %.16f -dm %.2f -pd %.16f -o %s %s" % (rfifind_mask, fold_period, dm, pdot, output_filename, filterbank_file)
    else:
        cmd = "prepfold -fixchi -noxwin -topo -n 64 -p %.16f -dm %.2f -pd %.16f -o %s %s" % (fold_period, dm, pdot, output_filename, filterbank_file)

    try:
        subprocess.check_output(cmd, shell=True)
        return (True, row['cand_id_in_file'])
    except subprocess.CalledProcessError as e:
        return (False, row['cand_id_in_file'], str(e))

def fold_with_presto(df, filterbank_file, tsamp, fft_size, source_name_prefix, prepfold_threads, rfifind_mask=None):
    #num_cores = min(cpu_count(), len(df))  # Use all available cores but no more than the number of rows
    num_cores = min(prepfold_threads, len(df))  # Use all available cores but no more than the number of rows
    args_list = [(row, filterbank_file, tsamp, fft_size, source_name_prefix, rfifind_mask) for _, row in df.iterrows()]

    pool = Pool(num_cores)
    results = pool.map(run_prepfold, args_list)
    pool.close()
    pool.join()

    for result in results:
        if not result[0]:  # If success is False
            print("Error with candidate ID %s: %s" % (result[1], result[2]))




# Prepfold serial version
# def fold_with_presto(df, filterbank_file, tsamp, fft_size, source_name_prefix, rfifind_mask=None):
#     for index, row in df.iterrows():
#         peasoup_period = row['period']
#         peasoup_acceleration = row['acc']
#         pdot = a_to_pdot(peasoup_period, peasoup_acceleration)
#         fold_period = period_correction_for_prepfold(peasoup_period, pdot, tsamp, fft_size)
#         output_filename =  source_name_prefix + '_Peasoup_fold_candidate_id_' + str(row['cand_id_in_file'])
#         dm = row['dm']
#         if rfifind_mask is not None:
#             cmd = "prepfold -fixchi -noxwin -topo -n 64 -mask %s -p %.16f -dm %.2f -pd %.16f -o %s %s" %(rfifind_mask, fold_period, dm, pdot, output_filename, filterbank_file)
#         else:
#             cmd = "prepfold -fixchi -noxwin -topo -n 64 -p %.16f -dm %.2f -pd %.16f -o %s %s" %(fold_period, dm, pdot, output_filename, filterbank_file)
#         subprocess.check_output(cmd, shell=True)


def fold_with_pulsarx(df, input_filenames, tsamp, fft_size, source_name_prefix, tstart, fast_nbins, slow_nbins, subint_length, nsubband, utc_beam, beam_name, pulsarx_threads, TEMPLATE, cmask=None):
    cand_dms = df['dm'].values
    cand_accs = df['acc'].values
    cand_period = df['period'].values
    pdot = a_to_pdot(cand_period, cand_accs)
    # Prepfold is used here on purpose. Period modified to beginning of tobs with epoch pointing to tstart
    cand_mod_period_beginning_tobs = period_correction_for_prepfold(cand_period, pdot, tsamp, fft_size)
    cand_mod_frequencies = 1/cand_mod_period_beginning_tobs
    cand_snrs = df['snr'].values
    pulsarx_predictor = generate_pulsarX_cand_file(cand_mod_frequencies, cand_dms, cand_accs, cand_snrs)
    nbins_string = "-b {} --nbinplan 0.1 {}".format(fast_nbins, slow_nbins)
    output_rootname = utc_beam + "_" + beam_name

    if 'ifbf' in beam_name:
        beam_tag = "--incoherent"
    elif 'cfbf' in beam_name:
        beam_tag = "-i {}".format(int(beam_name.strip("cfbf")))
    else:
        beam_tag = ""

    zap_string = ""
    if cmask is not None:
        cmask = cmask.strip()
        if cmask:
            try:
                zap_string = " ".join(["--rfi zap {} {}".format(
                    *i.split(":")) for i in cmask.split(",")])
            except Exception as error:
                raise Exception("Unable to parse channel mask: {}".format(
                    str(error)))
    
    script = "psrfold_fil --plotx -v -t {} --candfile {} -n {} {} {} --template {} --clfd 2.0 -L {} -f {} --rfi zdot {}-o {} --srcname {} --pepoch {}".format(
              pulsarx_threads, pulsarx_predictor, nsubband, nbins_string, beam_tag, TEMPLATE, subint_length, input_filenames, zap_string, output_rootname, source_name_prefix, tstart)
    subprocess.check_output(script, shell=True)
    #print(script)

def main():
    parser = argparse.ArgumentParser(description='Fold all candidates from Peasoup xml file')
    parser.add_argument('-o', '--output_path', help='Output path to save results',  default=os.getcwd(), type=str)
    parser.add_argument('-i', '--input_file', help='Name of the input xml file', type=str)
    parser.add_argument('-m', '--mask_file', help='Mask file for prepfold', type=str)
    parser.add_argument('-t', '--fold_technique', help='Technique to use for folding (presto or pulsarx)', type=str, default='presto')
    parser.add_argument('-n', '--nh', help='Filter candidates with nh value', type=int, default=0)
    parser.add_argument('-f', '--fast_nbins', help='High profile bin limit for slow-spinning pulsars', type=int, default=128)
    parser.add_argument('-s', '--slow_nbins', help='Low profile bin limit for fast-spinning pulsars', type=int, default=64)
    parser.add_argument('-sub', '--subint_length', help='Subint length (s)', type=int, default=10)
    parser.add_argument('-nsub', '--nsubband', help='Number of subbands', type=int, default=64)
    parser.add_argument('-b', '--beam_name', help='Beam name string', type=str, default='cfbf00000')
    parser.add_argument('-utc', '--utc_beam', help='UTC beam name string', type=str, default='2024-01-01-00:00:00')
    parser.add_argument('-c', '--chan_mask', help='Peasoup Channel mask file to be passed onto pulsarx', type=str, default='')
    parser.add_argument('-threads', '--pulsarx_threads', help='Number of threads to be used for pulsarx', type=int, default='24')
    parser.add_argument('-pthreads', '--presto_threads', help='Number of threads to be used for prepfold', type=int, default='12')
    parser.add_argument('-p', '--pulsarx_fold_template', help='Fold template pulsarx', type=str, default='meerkat_fold.template')

    args = parser.parse_args()

    if not args.input_file:
        print("You need to provide an xml file to read")
        sys.exit()

    xml_file = args.input_file
    tree = ET.parse(xml_file)
    root = tree.getroot()
    header_params = root[1]
    search_params = root[2]
    candidates = root[6]
    prepfold_threads = args.presto_threads
    filterbank_file = str(search_params.find("infilename").text)
    tsamp = float(header_params.find("tsamp").text)
    fft_size = int(search_params.find("size").text)
    nsamples = int(search_params.find("nsamples").text)
    tstart = float(header_params.find("tstart").text)
    source_name_prefix = str(header_params.find("source_name").text).strip()
  

    ignored_entries = ['candidate', 'opt_period', 'folded_snr', 'byte_offset', 'is_adjacent', 'is_physical', 'ddm_count_ratio', 'ddm_snr_ratio']
    rows = []
    for candidate in candidates:
        cand_dict = {}
        for cand_entry in candidate.iter():
            if not cand_entry.tag in ignored_entries:
                cand_dict[cand_entry.tag] = cand_entry.text
        cand_dict['cand_id_in_file'] = candidate.attrib.get("id")
        rows.append(cand_dict)

    df = pd.DataFrame(rows)
    df = df.astype({"snr": float, "dm": float, "period": float, "nh":int, "acc": float, "nassoc": int})
    df = df[df['nh'] >= args.nh]
    PulsarX_Template = args.pulsarx_fold_template

    if args.fold_technique == 'presto':
        fold_with_presto(df, filterbank_file, tsamp, fft_size, source_name_prefix, prepfold_threads)
    else:
        fold_with_pulsarx(df, filterbank_file, tsamp, fft_size, source_name_prefix, tstart, args.fast_nbins, args.slow_nbins, args.subint_length, args.nsubband, args.utc_beam, args.beam_name, args.pulsarx_threads, PulsarX_Template,  args.chan_mask)




if __name__ == "__main__":
    main()



    









