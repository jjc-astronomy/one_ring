import xml.etree.ElementTree as ET
import sys, os, subprocess
import argparse
import pandas as pd
import numpy as np

parser = argparse.ArgumentParser(description='Fold all candidates from Peasoup xml file')
parser.add_argument('-o', '--output_path', help='Output path to save results',  default=os.getcwd(), type=str)
parser.add_argument('-i', '--input_file', help='Name of the input xml file', type=str)
parser.add_argument('-m', '--mask_file', help='Mask file for prepfold', type=str)
parser.add_argument('-f', '--fold_technique', help='Technique to use for folding (presto or pulsarx)', type=str, default='presto')
parser.add_argument('-n', '--nh', help='Filter candidates with nh value', type=int, default=0)

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

filterbank_file = str(search_params.find("infilename").text)
tsamp = float(header_params.find("tsamp").text)
fft_size = int(search_params.find("size").text)

def generate_pulsarX_cand_file(
        tmp_dir,
        beam_name,
        utc_name,
        cand_mod_periods,
        cand_dms,
        cand_accs,
        cand_snrs):

    cand_file_path = '%s/%s_%s_cands.candfile' % (tmp_dir, beam_name, utc_name)
    source_name_prefix = "%s_%s" % (beam_name, utc_name)
    with open(cand_file_path, 'w') as f:
        f.write("#id DM accel F0 F1 S/N\n")
        for i in range(len(cand_mod_periods)):
            f.write(
                "%d %f %f %f 0 %f\n" %
                (i,
                 cand_dms[i],
                    cand_accs[i],
                    1.0 /
                    cand_mod_periods[i],
                    cand_snrs[i]))
        f.close()

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

os.chdir(args.output_path)

# Create an empty list to store the commands
#cmds_list = []

for index, row in df.iterrows():
    peasoup_period = row['period']
    peasoup_acceleration = row['acc']
    pdot = a_to_pdot(peasoup_period, peasoup_acceleration)
    if args.fold_technique == 'presto':
        fold_period = period_correction_for_prepfold(peasoup_period, pdot, tsamp, fft_size) 
    elif args.fold_technique == 'pulsarx':
        fold_period = period_correction_for_pulsarx(peasoup_period, pdot, tsamp, fft_size)

    output_filename = str(header_params.find("source_name").text).strip() + '_Peasoup_fold_candidate_id_' + str(row['cand_id_in_file'])
    dm = row['dm']
    if args.mask_file:
        cmd = "prepfold -fixchi -noxwin -topo -n 64 -mask %s -p %.16f -dm %.2f -pd %.16f -o %s %s" %(args.mask_file, fold_period, dm, pdot, output_filename, filterbank_file)
    else:
        cmd = "prepfold -fixchi -noxwin -topo -n 64 -p %.16f -dm %.2f -pd %.16f -o %s %s" %(fold_period, dm, pdot, output_filename, filterbank_file)
    subprocess.check_output(cmd, shell=True)

# Write all the commands to a file, with one command per line
# with open('commands.txt', 'w') as f:
#     for cmd in cmds_list:
#         f.write(f"{cmd}\n")
