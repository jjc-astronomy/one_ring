import os
import sys
import glob
import argparse
import logging
import hashlib
import uuid
import base64
import json
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy.engine import URL
from sqlalchemy import create_engine, MetaData, insert, select
import pandas as pd
import your  # Library to read PSRFITS and filterbank headers
import skyweaver  # Hypothetical library to parse BVRUSE metadata
from astropy.coordinates import SkyCoord
from astropy import units as u
import subprocess
from uuid_utils import UUIDUtility
import ast
import re
from collections import OrderedDict
import csv
from pathlib import Path


load_dotenv(dotenv_path=Path('.testdb.env'))


class WorkflowJSONBuilder:
    def __init__(self, pipeline_id, json_db_ids_filename):
        self.pipeline_id = pipeline_id
        self.json_db_ids_filename = json_db_ids_filename
        self.programs = []
        self.global_fields = {}  # New attribute for global fields

    def add_program_entry(self, program_name, program_id, metadata, arguments):
        # Check if this program entry already exists; if not, create it
        for p in self.programs:
            if p['program_name'] == program_name and p['program_id'] == program_id:
                return p
        new_prog = {
            "program_name": program_name,
            "program_id": program_id,
            "metadata": metadata,
            "arguments": arguments,
            "data_products": []
        }
        self.programs.append(new_prog)
        return new_prog

    def add_data_products_to_program(self, program_id, data_product_list):
        for p in self.programs:
            if p['program_id'] == program_id:
                p['data_products'].extend(data_product_list)
                return

    def add_global_field(self, key, value):
        self.global_fields[key] = value

    def to_json(self, indent=2, filename=None):
        # Group programs by program_name while preserving the original order
        #This is done for aesthetics only.
        grouped_programs = []
        seen_program_names = OrderedDict()

        for prog in self.programs:
            prog_name = prog['program_name']
            if prog_name not in seen_program_names:
                seen_program_names[prog_name] = []
            seen_program_names[prog_name].append(prog)

        for progs in seen_program_names.values():
            grouped_programs.extend(progs)

        # Create a dictionary with the grouped programs
        filename = filename if filename else self.json_db_ids_filename
        data = {
            "json_db_ids_filename": filename if filename else self.json_db_ids_filename,
            "pipeline_id": self.pipeline_id,
            "programs": grouped_programs
        }
        data.update(self.global_fields)
        

        with open(filename, 'w') as f:
            json.dump(data, f, indent=indent)




class DatabaseUploader:
    def __init__(self, db_host, db_port, db_username, db_password, db_name, 
                 project_name, telescope_name, 
                 hardware_name=None,
                 project_metadata_file=None, 
                 pointing_idx=None,
                 docker_hash_file=None,
                 nextflow_config_file=None,
                 verbose=False,
                 overrides=None):
        
        self.logger = logging.getLogger("DatabaseUploader")
        log_level = logging.DEBUG if verbose else logging.INFO
        self.logger.setLevel(log_level)

        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(message)s'))
            self.logger.addHandler(handler)
        
        self.project_name = project_name
        self.telescope_name = telescope_name
        self.hardware_name = hardware_name
        self.project_metadata_file = project_metadata_file
        self.docker_hash_file = docker_hash_file
        self.nextflow_config_file = nextflow_config_file
        self.pointing_idx = pointing_idx
        self.overrides = overrides if overrides else {}

        # DB credentials
        connection_url = URL.create(
            "mysql+mysqlconnector", 
            username=db_username, 
            password=db_password, 
            host=db_host, 
            database=db_name,
            port=db_port
        )
        self.engine = create_engine(connection_url, echo=False)
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)

        # Insert Project, Telescope, Hardware and get their IDs
        self.project_id = self._insert_project_name(self.project_name, return_id=True)
        self.telescope_id = self._insert_telescope_name(self.telescope_name, return_id=True)
        self.hardware_id = self._insert_hardware(self.hardware_name, return_id=True)

    def create_entry(self, name, description, program_params, extra_args=None):

        return {
            "program_name": name,
            "description": description,
            "container_image_name": program_params['container_image_name'],
            "container_image_path": program_params['container_image_path'],
            "container_image_version": program_params['container_image_version'],
            "container_image_id": program_params['container_image_id'],
            "container_type": program_params['container_type'],
            "extra_args": extra_args
        }
    def dump_lookup_table(self, table_name, output_filename):
        
        
        # Get a reference to the table using your existing helper
        lookup_table = self._get_table(table_name)

        # Build a SELECT statement for all rows
        stmt = select(lookup_table)
        
        # Execute the query and fetch all results
        with self.engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        # Extract column names
        columns = [col.name for col in lookup_table.columns]
        
        # Write out to CSV
        with open(output_filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Write header
            writer.writerow(columns)
            # Write each row
            for row in rows:
                writer.writerow(row)



    
    def upload_raw_data(self, csv_file):
        """
        Read the CSV and upload all data products.
        CSV columns:
        filenames,beam_name,utc_start,hardware_name

        'filenames' may contain multiple files separated by spaces.
        """
        self.logger.info(f"IMPORTANT: Ensure Your nf_config_for_data_upload.cfg is updated by running nextflow config -profile contra -flat -sort > nf_config_for_data_upload.cfg")
        self.logger.info(f"Uploading data from {csv_file}")
        df = pd.read_csv(csv_file)
        nextflow_cfg = self._parse_nextflow_flat_config_from_file(self.nextflow_config_file)
        
        # Validate required columns
        required = ['target','beam_name','utc_start','filenames','hardware_name']
        for r in required:
            if r not in df.columns:
                raise ValueError(f"CSV must contain column {r}")

        unique_pointings = df[['target','utc_start']].drop_duplicates()
        if len(unique_pointings) > 1:
            raise ValueError("CSV contains multiple pointings. Upload one target and pointing (utc_start) at a time.")
        
        # Get metadata for this pointing
        pointing_metadata = self._parse_metadata_for_pointing(df)

        # Insert target
        target_id = self._insert_target_name(
            pointing_metadata['target_name'],
            pointing_metadata['ra'],
            pointing_metadata['dec'],
            self.project_id,
            return_id=True
        )

        # Insert pointing
        pointing_id = self._insert_pointing(
            pointing_metadata['utc_start'],
            pointing_metadata['tobs'],
            pointing_metadata['nchans'],
            pointing_metadata['freq_band'],
            target_id,
            pointing_metadata['freq_start_mhz'],
            pointing_metadata['freq_end_mhz'],
            pointing_metadata['tsamp'],
            self.telescope_id,
            receiver_name=pointing_metadata.get('freq_band', None), #For MeerKAT, freq_band is receiver name
            return_id=True
        )

        #Insert pipeline
        repo_name, branch_name, last_commit_id = self._get_repo_details()
        #Insert pipeline
        pipeline_id = self._insert_pipeline(
            nextflow_cfg['pipeline_name'],
            repo_name,
            last_commit_id,
            branch_name,
            return_id=True
        )

        # Initialize JSON builder now that we have pipeline_id
        self.json_builder = WorkflowJSONBuilder(pipeline_id=pipeline_id, json_db_ids_filename=f"{pointing_metadata['target_name']}_pipeline_run.json")
        #Read docker hashes
        docker_hash_df = pd.read_csv(self.docker_hash_file)
        
        #Read filtool parameters
        filtool_params = self._prepare_program_parameters(nextflow_cfg, docker_hash_df, 'filtool', 'pulsarx')
        
        filtool_id = self._insert_filtool(
            filtool_params, 
            return_id=True)

        # Add filtool to JSON
        filtool_metadata = {
            "container_image_name": filtool_params['container_image_name'],
            "container_image_path": filtool_params['container_image_path'],
            "container_image_version": filtool_params['container_image_version'],
            "container_type": filtool_params['container_type']
        }
        # Keep only the arguments for filtool
        filtool_arguments = {k: v for k, v in filtool_params.items() if k not in ['program_name', 'container_image_name', 'container_image_version', 'container_type', 'container_image_id', 'container_image_path']}
        self.json_builder.add_program_entry("filtool", filtool_id, filtool_metadata, filtool_arguments)
        
        for _, row in df.iterrows():
            #Insert beam type
            beam_type_id = self._insert_beam_type(
                row['beam_type'], return_id=True)

            #Insert beam configuration
            beam_config_id = self._insert_beam_config(
                row['bf_ra'], 
                row['bf_dec'], 
                row['bf_reference_freq'],
                row['bf_utc'], 
                row['bf_tiling_method'], 
                row['bf_tiling_shape'],
                row['bf_overlap'], 
                row['beam_shape_x'], 
                row['beam_shape_y'],
                row['beam_shape_angle'], 
                return_id=True
            )
            #Insert beam
            beam_metadata = self._extract_file_header(row['filenames'].split()[0])
            beam_id = self._insert_beam(
                row['beam_name'],
                beam_metadata['ra_str'],
                beam_metadata['dec_str'],
                pointing_id,
                beam_type_id,
                beam_metadata['tsamp'],
                beam_config_id=beam_config_id,
                return_id=True
            )
            antenna_list = row['bf_sub_array'].split()
            antenna_list = [self._map_telescope_name_to_antenna_prefix(self.telescope_name) + a for a in antenna_list]

            for antenna in antenna_list:
                #Insert antenna
                antenna_id = self._insert_antenna(
                    antenna,
                    self.telescope_id,
                    return_id=True
                )
                #Insert beam_antenna linked table. No need to return ID
                self._insert_beam_antenna(
                    antenna_id,
                    beam_id
                )
            #Insert file type
            file_type_id = self._insert_file_type(
                beam_metadata['file_ext'],
                return_id=True
            )
            dp_id_list = []
            
            for filename in row['filenames'].split():
                dp_metadata = self._extract_file_header(filename)
                dp_id = self._insert_data_product(
                    beam_id,
                    file_type_id,
                    dp_metadata['filename'],
                    dp_metadata['filepath'],
                    available=1,
                    locked=0,
                    utc_start=dp_metadata['tstart_utc'],
                    tsamp_seconds=dp_metadata['tsamp'],
                    tobs_seconds=dp_metadata['tobs'],
                    nsamples=dp_metadata['nsamples'],
                    freq_start_mhz=dp_metadata['freq_start_mhz'],
                    freq_end_mhz=dp_metadata['freq_end_mhz'],
                    hardware_id=self.hardware_id,
                    nchans=dp_metadata['nchans'],
                    nbits=dp_metadata['nbits'],
                    mjd_start=dp_metadata['tstart_mjd'],
                    filehash=None,
                    metainfo=None,
                    coherent_dm=row['coherent_dm'] if 'coherent_dm' in row else None,
                    subband_dm=row['subband_dm'] if 'subband_dm' in row else None,
                    return_id=True
                )
                dp_id_list.append(dp_id)
                
            #Check if this observation is processed in a different cluster.
            #Default is Contra, unless user over-rides it in the CSV
            beam_hardware = row['hardware_name'] if 'hardware_name' in row else 'contra'
            if beam_hardware.lower() != self.hardware_name.lower():
                self.logger.debug(f"Beam {row['beam_name']} is processed in a different cluster: {beam_hardware}")
                beam_hardware_id = self._insert_hardware(beam_hardware, return_id=True)
            else:
                beam_hardware_id = self.hardware_id
            
            
            pipeline_cdm = (row['subband_dm'] if 'subband_dm' in row 
            else row['coherent_dm'] if 'coherent_dm' in row 
            else 0.0)
            
            data_products_for_filtool = []
            data_products_for_filtool.append({
                "target_name": pointing_metadata['target_name'],
                "utc_start": pointing_metadata['utc_start'].split('.')[0] if '.' in pointing_metadata['utc_start'] else pointing_metadata['utc_start'],
                "filstr": row['filstr'] if 'filstr' in row else self.project_name,
                "pointing_id": pointing_id,
                "beam_name": row['beam_name'],
                "beam_id": beam_id,
                "hardware": beam_hardware,
                "hardware_id": beam_hardware_id,
                "coherent_dm": pipeline_cdm,
                "filenames": row['filenames'],
                "dp_id": dp_id_list
            })
            self.json_builder.add_data_products_to_program(filtool_id, data_products_for_filtool)

            

        
        #Get the full observation tobs and nsamples
        full_obs_metadata = self._extract_file_header(row['filenames'].split())
        
        peasoup_params = self._store_peasoup_configs(nextflow_cfg, docker_hash_df, df)
        
        # Add each peasoup record as a separate program entry in JSON
        # peasoup_records is a list of dicts, each representing a peasoup configuration with arguments and peasoup_id
        for peasoup_record in peasoup_params:
            peasoup_metadata = {
                "container_image_name": peasoup_record['container_image_name'],
                "container_image_path": peasoup_record['container_image_path'],
                "container_image_version": peasoup_record['container_image_version'],
                "container_type": peasoup_record['container_type']
            }
            #Keep only arguments for peasoup. Extra field of program_id here since this was generated by _store_peasoup_configs
            arguments = {k: v for k, v in peasoup_record.items() if k not in ['program_name', 'program_id', 'container_image_name', 'container_image_version', 'container_type', 'container_image_id', 'container_image_path']}
            self.json_builder.add_program_entry("peasoup", peasoup_record['program_id'], peasoup_metadata, arguments)
            self.logger.debug(f"Added peasoup program entry: {peasoup_record['program_id']}")
            '''
            PulsarX arguments depends on peasoup arguments (start_sample, fft_size, nsamples, pepoch). So we generate it in the loop.
            They are are matched in the nextflow pipeline based on pepoch.
            '''
            pulsarx_params = self._prepare_program_parameters(nextflow_cfg, docker_hash_df, 'pulsarx', 'pulsarx')
            
            #Calculate pepoch, start_fraction, end_fraction, and effective tobs
            pepoch, start_fraction, end_fraction, effective_tobs = self._calculate_pepoch_start_end_fractions(full_obs_metadata, peasoup_record['start_sample'], peasoup_record['fft_size'])
            
            
            #If subint is null, set it to effective tobs/64
            if pulsarx_params.get('subint_length') == "null":
                pulsarx_params['subint_length'] = int(effective_tobs/64)
                
            pulsarx_params['pepoch'] = pepoch
            pulsarx_params['start_frac'] = start_fraction
            pulsarx_params['end_frac'] = end_fraction
            pulsarx_params['template_path'] = os.path.dirname(pulsarx_params['template_file'])
            pulsarx_params['template_file'] = os.path.basename(pulsarx_params['template_file'])
            
            pulsarx_metadata = {
                "container_image_name": pulsarx_params['container_image_name'],
                "container_image_path": pulsarx_params['container_image_path'],
                "container_image_version": pulsarx_params['container_image_version'],
                "container_type": pulsarx_params['container_type']
            }
            pulsarx_id = self._insert_pulsarx(
                pulsarx_params,
                return_id=True
            ) 
            #Keep only arguments for pulsarx
            pulsarx_arguments = {k: v for k, v in pulsarx_params.items() if k not in ['program_name', 'container_image_name', 'container_image_version', 'container_type', 'container_image_id', 'container_image_path']}
            self.json_builder.add_program_entry("pulsarx", pulsarx_id, pulsarx_metadata, pulsarx_arguments)
        
        
        #ML Model Scoring
        try:
            # Convert to integer
            nextflow_cfg['candidate_filter.ml_candidate_scoring.enable'] = int(nextflow_cfg['candidate_filter.ml_candidate_scoring.enable'])
            
            # Check if it's 0 or 1
            if nextflow_cfg['candidate_filter.ml_candidate_scoring.enable'] not in (0, 1):
                raise ValueError("Invalid value for candidate_filter.ml_candidate_scoring.enable. Value must be 0 or 1")
            if nextflow_cfg['candidate_filter.ml_candidate_scoring.enable'] == 1:
                logging.info("ML Candidate Scoring is enabled. Adding ML models into the pipeline.")
                #Read ML Model Parameters
                ml_model_params = self._store_ml_models_configs(nextflow_cfg, docker_hash_df)
                
        except:
            raise ValueError("Invalid value for candidate_filter.ml_candidate_scoring.enable. Must be 0 or 1")
        
        # Post folding heuristics optional calculation
        
        try:
            nextflow_cfg['candidate_filter.calculate_post_folding_heuristics.enable'] = int(nextflow_cfg['candidate_filter.calculate_post_folding_heuristics.enable'])

            if nextflow_cfg['candidate_filter.calculate_post_folding_heuristics.enable'] not in (0, 1):
                raise ValueError("Invalid integer value for candidate_filter.calculate_post_folding_heuristics.enable. Value must be 0 or 1")

            if nextflow_cfg['candidate_filter.calculate_post_folding_heuristics.enable'] == 1:
                
                logging.info("Alpha, Beta, Gamma calculation is enabled. Inserting into candidate filter")
                alpha_params = self.create_entry("alpha", "S/N at ZERO DM/S/N at candidate DM", filtool_params)
                beta_params = self.create_entry("beta", "ABS(DM_FFT - DM_FOLD)/DM_FFT", filtool_params)
                gamma_params = self.create_entry("gamma", "DM_ERR/DM_FOLD", filtool_params)
                delta_params = self.create_entry("delta", "sqrt((P_FFT - P_FOLD)**2 + (Pdot_FFT - Pdot_FOLD)**2)", filtool_params)
                
                #Insert candidate filter
                alpha_id = self._insert_candidate_filter(alpha_params, return_id=True)
                beta_id = self._insert_candidate_filter(beta_params, return_id=True)
                gamma_id = self._insert_candidate_filter(gamma_params, return_id=True)
                delta_id = self._insert_candidate_filter(delta_params, return_id=True)
        except:
            raise ValueError("Invalid value for candidate_filter.calculate_post_folding_heuristics.enable. Must be an integer (0 or 1)")

        # CandyPicker optional search filter
        try:
            nextflow_cfg['candidate_filter.candy_picker.enable'] = int(nextflow_cfg['candidate_filter.candy_picker.enable'])
            if nextflow_cfg['candidate_filter.candy_picker.enable'] not in (0, 1):
               
                raise ValueError("Invalid value for candidate_filter.candy_picker.enable. Value must be 0 or 1")

            if nextflow_cfg['candidate_filter.candy_picker.enable'] == 1:
                try:
                    period_tolerance = float(nextflow_cfg['candidate_filter.candy_picker.period_tolerance'])
                except:
                    raise ValueError("Invalid value for candidate_filter.candy_picker.period_tolerance. Must be a float")
                try:
                    dm_tolerance = float(nextflow_cfg['candidate_filter.candy_picker.dm_tolerance'])
                except:
                    raise ValueError("Invalid value for candidate_filter.candy_picker.dm_tolerance. Must be a float")

                candy_picker_extra_args = f"-p {period_tolerance} -d {dm_tolerance}"
               
                
                logging.info("CandyPicker is enabled. Inserting into candidate filter")
                candy_picker_params = self._prepare_program_parameters(nextflow_cfg, docker_hash_df, 'candy_picker', 'candy_picker')
                candy_picker_params = self.create_entry("candy_picker", "Removes similar search candidates based on period and dm tolerance", candy_picker_params, extra_args=candy_picker_extra_args)
                candy_picker_id = self._insert_candidate_filter(candy_picker_params, return_id=True)
                
                #Add candy_picker_groups to main JSON
                candy_picker_groups = df.groupby('coherent_dm')['subband_dm'].agg(['min', 'max'])
                candy_picker_groups = [[row['min'], row['max']] for _, row in candy_picker_groups.iterrows()]
                # Add candy_picker_groups to main JSON
                self.json_builder.add_global_field("candy_picker_groups", candy_picker_groups)

               
        except:
            raise ValueError("Invalid value for candidate_filter.candy_picker.enable. Must be an integer (0 or 1)")
        
       
        
        #Dump JSON
        self.json_builder.to_json()

    
    def _calculate_pepoch_start_end_fractions(self, full_obs_metadata, start_sample, fft_size):
        """
        Calculate pepoch, start_fraction, and end_fraction from observation metadata.
        start_fraction = start_sample / nsamples
        end_fraction = (start_sample + fft_size) / nsamples

        pepoch logic:
        start_time_days = (start_sample * tsamp) / 86400
        end_time_days   = (fft_size * tsamp) / 86400
        tstart_updated = tstart_mjd + start_time_days
        pepoch = tstart_updated + 0.5 * end_time_days
        """
        nsamples = full_obs_metadata['nsamples']
        tsamp = full_obs_metadata['tsamp']
        tstart_mjd = full_obs_metadata['tstart_mjd']
        

        #This is used for folding.Round to 3 decimal places
        start_fraction = round(start_sample / nsamples, 3)

        end_fraction = round((start_sample + fft_size) / nsamples, 3)
        if end_fraction > 1.0:
            self.logger.debug(f"End fraction is greater than 1.0. Likely because your padding greater than tobs. Setting it 1.0 for folding.")
            end_fraction = 1.0

        
        #pepoch calculation depends on peasoup fft size and not necessarily observation nsamples. You can pad with more samples than tobs.
        start_time_days = (start_sample * tsamp) / 86400.0
        end_time_days = (fft_size * tsamp) / 86400.0
        tstart_updated = tstart_mjd + start_time_days
        pepoch = round(tstart_updated + 0.5 * end_time_days, 6)
        effective_tobs = (end_fraction - start_fraction) * full_obs_metadata['tobs']
       

        return pepoch, start_fraction, end_fraction, effective_tobs

    def _store_peasoup_configs(self, nextflow_cfg, docker_hash_df, df):
        """
        - Reads peasoup configs (JSON string) and dd_plan (colon-sep strings).
        - Matches filterbank rows by coherent DM.
        - Inserts each peasoup configuration into DB.
        - Returns a list of dicts with all peasoup configurations and program name and ID.
        """
        # Base container fields for peasoup
        peasoup_params = self._prepare_program_parameters(nextflow_cfg, docker_hash_df, 'peasoup', 'peasoup')
        
        # Decode peasoup JSON
        raw_peasoup_str = nextflow_cfg['peasoup']
        peasoup_str = bytes(raw_peasoup_str, "utf-8").decode("unicode_escape").strip()
        peasoup_configs = json.loads(peasoup_str)  # list of dicts
        
        # dd_plan is colon-separated, parse via literal_eval
        ddplan_list = ast.literal_eval(nextflow_cfg['dd_plan'])  # e.g. ["0.0:0.0:100.0:1:1", "120:110:130:0.5:1", ...]

        # We'll collect all peasoup DB entries in a list
        all_peasoup_records = []

        for dd_str in ddplan_list:
            coherent_dm, start_dm, end_dm, dm_step, tscrunch = self._process_ddplan_entry(dd_str)

            # For each row in your filterbank DataFrame, match the DM
            for _, row in df.iterrows():
                cdm_data = row.get('subband_dm') or row.get('coherent_dm') or 0.0
                if float(cdm_data) == float(coherent_dm):
                    # For each peasoup config block in peasoup_configs
                    for peasoup_cfg in peasoup_configs:
                        # Create a fresh dict to insert, merging peasoup_params + dd_plan fields + peasoup_cfg
                        entry = dict(peasoup_params)

                        # Convert/assign the peasoup_cfg fields to match the MySQL schema
                        entry['acc_start']        = float(peasoup_cfg['acc_start']) if peasoup_cfg['acc_start'] is not None else None
                        entry['acc_end']          = float(peasoup_cfg['acc_end']) if peasoup_cfg['acc_end'] is not None else None
                        entry['acc_pulse_width']  = float(peasoup_cfg['acc_pulse_width']) if peasoup_cfg['acc_pulse_width'] is not None else None
                        entry['dm_pulse_width']   = float(peasoup_cfg['dm_pulse_width']) if peasoup_cfg['dm_pulse_width'] is not None else None
                        entry['min_snr']          = float(peasoup_cfg['min_snr']) if peasoup_cfg['min_snr'] is not None else None
                        entry['min_freq']        = float(peasoup_cfg['min_freq']) if peasoup_cfg['min_freq'] is not None else 0.1
                        entry['max_freq']        = float(peasoup_cfg['max_freq']) if peasoup_cfg['max_freq'] is not None else 1100.0
                        entry['ram_limit_gb']     = float(peasoup_cfg['ram_limit_gb']) if peasoup_cfg['ram_limit_gb'] is not None else None
                        entry['nharmonics']       = int(peasoup_cfg['nh']) if peasoup_cfg['nh'] is not None else None
                        entry['ngpus']            = int(peasoup_cfg['ngpus']) if peasoup_cfg['ngpus'] is not None else None
                        entry['total_cands_limit']= int(peasoup_cfg['total_cands']) if peasoup_cfg['total_cands'] is not None else None
                        entry['fft_size']         = int(peasoup_cfg['fft_size']) if peasoup_cfg['fft_size'] is not None else None
                        entry['accel_tol']        = float(peasoup_cfg['accel_tol']) if peasoup_cfg['accel_tol'] is not None else None
                        entry['birdie_list']      = peasoup_cfg['birdie_list']
                        entry['chan_mask']        = peasoup_cfg['chan_mask']
                        entry['nsamples']         = int(peasoup_cfg['nsamples']) if peasoup_cfg['nsamples'] is not None else None
                        entry['start_sample']     = int(peasoup_cfg['start_sample']) if peasoup_cfg['start_sample'] is not None else None
                        entry['extra_args']       = peasoup_cfg['extra_args']

                        # dd_plan info
                        entry['dm_start'] = float(start_dm)
                        entry['dm_end']   = float(end_dm)
                        entry['dm_step']  = float(dm_step)
                        entry['coherent_dm'] = float(coherent_dm)
                        
                        
                        # Insert into DB, retrieve ID
                        peasoup_id = self._insert_peasoup(entry, return_id=True)
                        entry['program_name'] = 'peasoup'
                        entry['program_id'] = peasoup_id
                        entry['cfg_name'] = peasoup_cfg['cfg_name']
                        
                        #json_entry = dict(entry)
                        #json_entry['data_products'] = []  # no data_products yet
                        # Save this fully resolved entry (with peasoup_id) into our list
                        all_peasoup_records.append(entry)

        
        return all_peasoup_records
                        
    def _parse_nextflow_flat_config_from_file(self, file_path):

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
        
        nextflow_cfg = {key.replace('params.', '') if key.startswith('params.') else key: value 
              for key, value in config.items()}

        return nextflow_cfg
    
    def _prepare_program_parameters(self, params, docker_image_hashes, program_name, image_key):
        """
        Prepares program configurations based on the provided Nextflow configuration.

        :param params: Dictionary of Nextflow parameters.
        :param docker_image_hashes: DataFrame containing image hashes and versions.
        :param program_name: Name of the program (e.g., 'filtool' or 'peasoup').
        :param image_key: Key to look up the image in docker_image_hashes.
        :return: Dictionary of prepared parameters ready for database insertion.
        """
        # Extract program-specific parameters
        program_params = {k.replace(f"{program_name}.", ""): v for k, v in params.items() if k.startswith(f"{program_name}.")}
        
        # Add common fields
        program_params.update({
            'program_name': program_name,
            'container_image_name': os.path.basename(params.get(f"apptainer_images.{image_key}", "")),
            'container_image_path': os.path.dirname(params.get(f"apptainer_images.{image_key}", "")),
            'container_image_id': docker_image_hashes.loc[docker_image_hashes['Image'] == image_key, 'SHA256'].values[0],
            'container_image_version': docker_image_hashes.loc[docker_image_hashes['Image'] == image_key, 'Version'].values[0],
            'container_type': "apptainer"
        })

        # Conditionally add optional fields (For Candidate filters like PICS)
        if 'filename' in params:
            program_params['filename'] = params['filename']
        if 'filepath' in params:
            program_params['filepath'] = params['filepath']

        return program_params

    def _store_ml_models_configs(self, nextflow_cfg, docker_hash_df):
        """
        - Reads ml_models configs (JSON string).
        - Inserts each ml_models configuration into DB.
        - Returns a list of dicts with all ml_models configurations and program name and ID.
        """
        # Base container fields for ml_models
        ml_models_params = self._prepare_program_parameters(nextflow_cfg, docker_hash_df, 'pics', 'pics')
        
        
        # Decode ml_models JSON
        all_ml_models_records = []
        model_dir = nextflow_cfg['candidate_filter.ml_candidate_scoring.models_dir']
        ml_models = glob.glob(f"{model_dir}/*.pkl")

        if len(ml_models) == 0:
            raise ValueError("No ML models found in the directory")
        for ml_model in ml_models:
            # For each ml_models config block in ml_models_configs
            entry = dict(ml_models_params)
            entry['filename'] = os.path.basename(ml_model)
            entry['filepath'] = os.path.dirname(ml_model)
            
            # Insert into DB, retrieve ID
            candidate_filter_id = self._insert_candidate_filter(entry, return_id=True, generate_filehash=True)
            entry['program_id'] = candidate_filter_id
            all_ml_models_records.append(entry)

        return all_ml_models_records




    def _process_ddplan_entry(self, ddplan_entry):
        try:
            parts = ddplan_entry.split(':')
            if len(parts) != 5:
                raise ValueError(f"Invalid format for ddplan entry: '{ddplan_entry}'. Expected exactly 5 fields separated by ':'.")
            
            coherent_dm, start_dm, end_dm, dm_step, tscrunch = map(float, parts)
            
            self.logger.debug(f"Processed ddplan entry successfully: {ddplan_entry}")
            return coherent_dm, start_dm, end_dm, dm_step, tscrunch

        except ValueError as e:
            self.logger.error(f"Error: {e}")
            raise

    def _parse_metadata_for_pointing(self, df):
        """
        Parse metadata for a single pointing from a dataframe

        Logic:
        - Extract RA/Dec, antennas from metadata file (if project name like compact or trapum or meertime).
        - If overrides for RA/Dec/antennas provided, they take precedence.

        Returns a dictionary with keys:
        'antennas', 'ra', 'dec', 'target_name', 'utc_start', 'tsamp', 'tobs', 'nsamples', 'freq_start_mhz', 'freq_end_mhz', 'nchans', 'nbits', 'freq_band'
        """
        pointing_metadata = {}
        # Extract metadata based on project_type
        if self.project_name.lower() == "compact":
            bvruse_info = self._parse_bvruse_metadata()
            pointing_metadata['antennas'] = bvruse_info['antennas']
            pointing_metadata['ra'] = bvruse_info['phase_center_ra']
            pointing_metadata['dec'] = bvruse_info['phase_center_dec']
        
        #Get metadata from first beam file
        pointing_metadata['target_name'] = df.iloc[0]['target']
        pointing_metadata['utc_start'] = self._format_utc_start(df.iloc[0]['utc_start'])
        file_list = df.iloc[0]['filenames'].split()
        beam_metadata = self._extract_file_header(file_list)
        #Exclude RA/DEC from beam metadata, add rest to pointing_metadata
        exclude_keys = {'ra_deg', 'dec_deg', 'ra_str', 'dec_str', 'filename', 'filepath'}
        pointing_metadata.update({k: v for k, v in beam_metadata.items() if k not in exclude_keys})
          

        # Apply overrides: RA, DEC, antennas if provided
        if 'ra' in self.overrides:
            pointing_metadata['ra'] = self.overrides['ra']
        if 'dec' in self.overrides:
            pointing_metadata['dec'] = self.overrides['dec']
        if 'antennas' in self.overrides:
            pointing_metadata['antennas'] = self.overrides['antennas']

        # Determine freq_band if not overridden
        if 'freq_band' in self.overrides:
            pointing_metadata['freq_band'] = self.overrides['freq_band']
        else:
            if self.telescope_name.lower() == "meerkat":
                pointing_metadata['freq_band'] = self._get_freq_band(pointing_metadata['freq_start_mhz'], pointing_metadata['freq_end_mhz'])
            else:
                pointing_metadata['freq_band'] = "UNKNOWN"

        # If target_name is overridden
        if 'target_name' in self.overrides:
            pointing_metadata['target_name'] = self.overrides['target_name']

        return pointing_metadata

    def _format_utc_start(self, utc_str):
        # Ensure fractional seconds if not present
        if '.' not in utc_str:
            utc_str += '.0'
        return utc_str
    
    def _map_telescope_name_to_antenna_prefix(self, telescope_name):
        """
        Map telescope name to antenna prefix.
        """
        if telescope_name.lower() == "meerkat":
            return "m"
        elif telescope_name.lower() == "effelsberg":
            return "EF"
        elif telescope_name.lower() == "parkes":
            return "PK"
        elif telescope_name.lower() == "GBT":
            return "GB"
        else:
            return "unknown"
    def _parse_bvruse_metadata(self):
        if self.project_metadata_file is None:
            raise ValueError("No BVRUSE metadata file provided")
        sm = skyweaver.SessionMetadata.from_file(self.project_metadata_file)
        pointings = sm.get_pointings()
        if self.pointing_idx is None and len(pointings) > 1:
            raise ValueError("Multiple pointings found but no pointing_idx specified")

        idx = 0 if self.pointing_idx is None else int(self.pointing_idx)
        if idx >= len(pointings):
            raise ValueError("pointing_idx out of range")

        target = pointings[idx]
        antennas = sorted(target.session_metadata.antenna_positions.keys())
        ra = str(getattr(target.phase_centre.body, '_ra', '00:00:00'))
        dec = str(getattr(target.phase_centre.body, '_dec', '00:00:00'))
        utc_start = getattr(target.start_epoch, 'isot', '1970-01-01T00:00:00.0')

        return {
            'antennas': antennas,
            'phase_center_ra': ra,
            'phase_center_dec': dec,
            'utc_start': utc_start
        }

    def _parse_trapum_metadata(self):
        # Pseudocode for trapum metadata parsing
        with open(self.project_metadata_file) as f:
           data = json.load(f)
        
        # Extract RA/Dec/antennas from data
        # Just a placeholder
        return {
            'target_name': 'TrapumTarget',
            'ra': '17:45:40.04',
            'dec': '-29:00:28.1',
            'antennas': ['m000','m001','m002'],
            'freq_start_mhz': 816.0,
            'freq_end_mhz': 856.0,
            'tsamp_seconds':0.0003,
            'tobs': 120.0,
            'nchans':4096,
            'beam_type_name':'coherent',
            'receiver':'UHF'
        }

    def _parse_meertime_metadata(self):
        # Pseudocode for meertime parsing
        # Suppose we parse obs.header:
        # ra = ...
        # dec = ...
        # antennas = ...
        # freq, tsamp, ...
        return {
            'target_name': 'MeerTIME_Target',
            'ra': '13:25:27.6',
            'dec': '-43:01:08.6',
            'antennas': ['m000','m001'],
            'freq_start_mhz': 1284.0,
            'freq_end_mhz': 1360.0,
            'tsamp_seconds':0.0002,
            'tobs': 300.0,
            'nchans':4096,
            'beam_type_name':'coherent',
            'receiver':'L-band'
        }

    def _get_freq_band(self, freq_start_mhz, freq_end_mhz):
        center_freq = (freq_start_mhz + freq_end_mhz)/2
        if center_freq > 2000:
            return "SBAND"
        elif center_freq > 1000:
            return "LBAND"
        else:
            return "UHF"

    def _parse_and_format_datetime(datetime_str):
        """
        Parse the given datetime string and return a formatted string representation.

        Parameters:
        - datetime_str: The datetime string to parse.

        Returns:
        - A string representation of the datetime, with fractional seconds.
        """

        # Check for the presence of fractional seconds and append '.0' if absent
        has_fractional_seconds = '.' in datetime_str
        if has_fractional_seconds:
            return datetime_str
        else:
            datetime_str += '.0'

        return datetime_str
    

    def _extract_file_header(self, file_path):
        """
        Extract header information from a list of files. Files can be PSRFITS or filterbank. 
        They are assumed to be separated only in time.
        """

     
        if not isinstance(file_path, list):
            file_path = [file_path]

        #Sort by filename
        file_path.sort()
        # Extract the first header
        y = your.Your(file_path[0])
        h = y.your_header
        
        header = {
            'filename': os.path.basename(file_path[0]),
            'filepath': os.path.dirname(file_path[0]),
            'tsamp': h.tsamp,
            'tobs': h.tsamp * h.nspectra,
            'nsamples': h.nspectra,
            'freq_start_mhz': h.center_freq - abs(h.bw) / 2,
            'freq_end_mhz': h.center_freq + abs(h.bw) / 2,
            'nchans': h.nchans,
            'nbits': h.nbits,
            'ra_deg': getattr(h, 'ra_deg', 0.0),
            'dec_deg': getattr(h, 'dec_deg', 0.0),
            'file_ext': os.path.splitext(file_path[0])[1].lstrip('.'),
            "tstart_utc": h.tstart_utc.replace("T", " "),
            "tstart_mjd": h.tstart
        }
        #Use astropy to convert ra_deg and dec_deg to ra_str and dec_str
        coord = SkyCoord(ra=header['ra_deg']*u.degree, dec=header['dec_deg']*u.degree, frame='icrs')
        header['ra_str'] = coord.ra.to_string(unit=u.hour, sep=':', precision=2, pad=True)
        header['dec_str'] = coord.dec.to_string(unit=u.degree, sep=':', precision=1, alwayssign=True, pad=True)
        
        # Accumulate tobs for the rest of the files
        for path in file_path[1:]:
            y = your.Your(path)
            h = y.your_header
            header['tobs'] += h.tsamp * h.nspectra
            header['nsamples'] += h.nspectra

        return header
    

    def _combine_metadata(self, file_header, meta, overrides):
        combined = file_header.copy()
        for k,v in meta.items():
            if k not in combined:
                combined[k] = v
        if overrides:
            for k,v in overrides.items():
                combined[k] = v

        ext = os.path.splitext(file_header['filename'])[1].lstrip('.')
        combined['file_type'] = ext if ext else 'unknown'
        return combined

    def _get_table(self, name):
        return self.metadata.tables[name]
    
    def _get_id_from_name(table_name, name, alternate_key=None):

        '''
        Get the id for a given name in a given table
        '''
        table = _get_table(table_name)  

        with self.engine.connect() as conn:
            if alternate_key is not None:
                # Check if alternate key is a valid column name in the table
                if alternate_key not in table.c:
                    raise ValueError(f"Alternate key {alternate_key} not found in table {table_name}")
                stmt = select(table).where(getattr(table.c, alternate_key) == name).limit(1)
            else:
                # Assuming 'name' is a column in the table, and you are searching for a record with this column equal to the 'name' value
                stmt = select(table).where(table.c.name == name).limit(1)
            
            try:
                result = conn.execute(stmt).first()
                return result[0]
            
            except:
                raise ValueError(f"{name} does not exist in {table_name} table. Please add {name} first.")
    
    def _get_repo_details(self):
        """
        Get the current repository details: repo name, branch name, and last commit ID.
        Raises ValueError if any detail cannot be retrieved.
        """
        try:
            # Get the remote repository URL
            remote_url = subprocess.check_output(
                ["git", "config", "--get", "remote.origin.url"],
                universal_newlines=True
            ).strip()
        except subprocess.CalledProcessError:
            raise ValueError("Could not find the remote URL for the repository.")

        # Extract repo name
        repo_name = remote_url.split('/')[-1].replace('.git', '')

        try:
            # Get the current branch name
            branch_name = subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                universal_newlines=True
            ).strip()
        except subprocess.CalledProcessError:
            raise ValueError("Could not determine the current branch name.")

        try:
            # Get the last commit ID
            last_commit_id = subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                universal_newlines=True
            ).strip()
        except subprocess.CalledProcessError:
            raise ValueError("Could not determine the last commit ID.")

        return repo_name, branch_name, last_commit_id

    
    ### Database Insert Methods ###
    def _insert_project_name(self, project_name, description=None, return_id=False):
        table = self._get_table("project")
        with self.engine.connect() as conn:
            stmt = select(table).where(table.c.name == project_name).limit(1)
            res = conn.execute(stmt).first()
            if res is None:
                stmt = insert(table).values(name=project_name, description=description)
                db_update = conn.execute(stmt)
                conn.commit()
                self.logger.debug(f"Added {project_name} to project table")
                if return_id:
                    return db_update.lastrowid
            else:
                self.logger.debug(f"{project_name} exists in project table. Skipping...")
                if return_id:
                    return res[0]

    def _insert_telescope_name(self, telescope_name, description=None, return_id=False):
        table = self._get_table("telescope")
        with self.engine.connect() as conn:
            stmt = select(table).where(table.c.name == telescope_name).limit(1)
            res = conn.execute(stmt).first()
            if res is None:
                stmt = insert(table).values(name=telescope_name, description=description)
                db_update = conn.execute(stmt)
                conn.commit()
                self.logger.debug(f"Added {telescope_name} to telescope table")
                if return_id:
                    return db_update.lastrowid
            else:
                self.logger.debug(f"{telescope_name} exists in telescope table. Skipping...")
                if return_id:
                    return res[0]

    def _insert_hardware(self, hardware_name, job_scheduler=None, description=None, return_id=False):
        table = self._get_table("hardware")
        with self.engine.connect() as conn:
            stmt = select(table).where(table.c.name == hardware_name).limit(1)
            res = conn.execute(stmt).first()
            if res is None:
                stmt = insert(table).values(name=hardware_name, description=description, job_scheduler=job_scheduler)
                db_update = conn.execute(stmt)
                conn.commit()
                self.logger.debug(f"Added {hardware_name} to hardware table")
                if return_id:
                    return db_update.lastrowid
            else:
                self.logger.debug(f"{hardware_name} exists in hardware table. Skipping...")
                if return_id:
                    return res[0]

    def _insert_target_name(self, target_name, ra, dec, project_id, description=None, return_id=False):
        table = self._get_table("target")
        with self.engine.connect() as conn:
            stmt = (select(table)
                    .where(table.c.target_name == target_name)
                    .where(table.c.ra == ra)
                    .where(table.c.dec == dec)
                    .where(table.c.project_id == project_id)
                    .limit(1))
            res = conn.execute(stmt).first()
            if res is None:
                stmt = insert(table).values(
                    target_name=target_name, ra=ra, dec=dec, notes=description, project_id=project_id
                )
                db_update = conn.execute(stmt)
                conn.commit()
                self.logger.debug(f"Added {target_name} to target table")
                if return_id:
                    return db_update.lastrowid
            else:
                self.logger.debug(f"{target_name} exists in target table. Skipping...")
                if return_id:
                    return res[0]

    def _insert_pointing(self, utc_start_str, tobs, nchans, freq_band, target_id, freq_start_mhz, freq_end_mhz, tsamp_seconds, telescope_id, receiver_name=None, return_id=False):
        table = self._get_table("pointing")
        with self.engine.connect() as conn:
            utc_start = datetime.strptime(utc_start_str, '%Y-%m-%d-%H:%M:%S.%f').replace(microsecond=0)
            stmt = (select(table)
                    .where(table.c.utc_start == utc_start)
                    .where(table.c.target_id == target_id)
                    .where(table.c.telescope_id == telescope_id)
                    .where(table.c.freq_band == freq_band)
                    .limit(1))
            res = conn.execute(stmt).first()
            if res is None:
                stmt = insert(table).values(
                    utc_start=utc_start, tobs=tobs, nchans_raw=nchans, freq_band=freq_band,
                    target_id=target_id, freq_start_mhz=freq_start_mhz, freq_end_mhz=freq_end_mhz,
                    tsamp_raw=tsamp_seconds, telescope_id=telescope_id, receiver=receiver_name
                )
                db_update = conn.execute(stmt)
                conn.commit()
                self.logger.debug(f"Added pointing for target_id {target_id}")
                if return_id:
                    return db_update.lastrowid
            else:
                self.logger.debug(f"Pointing exists. Skipping...")
                if return_id:
                    return res[0]

    def _insert_beam_config(self, tiling_center_ra, tiling_center_dec, reference_freq, tiling_utc, tiling_method, tiling_shape, overlap, beam_shape_x, beam_shape_y, beam_shape_angle, coordinate_type="equatorial", return_id=False):
        table = self._get_table("beam_config")
        tiling_utc = datetime.strptime(tiling_utc, '%Y.%m.%d %H:%M:%S.%f').replace(microsecond=0)
        
        with self.engine.connect() as conn:
            stmt = (select(table)
                    .where(table.c.tiling_center_ra == tiling_center_ra)
                    .where(table.c.tiling_center_dec == tiling_center_dec)
                    .where(table.c.reference_freq == reference_freq)
                    .where(table.c.tiling_utc == tiling_utc)
                    .where(table.c.tiling_method == tiling_method)
                    .where(table.c.tiling_shape == tiling_shape)
                    .where(table.c.overlap == overlap)
                    .where(table.c.beam_shape_x == beam_shape_x)
                    .where(table.c.beam_shape_y == beam_shape_y)
                    .where(table.c.beam_shape_angle == beam_shape_angle)
                    .limit(1))
            res = conn.execute(stmt).first()
           
            if res is None:
                stmt = insert(table).values(
                    tiling_center_ra=tiling_center_ra, tiling_center_dec=tiling_center_dec,
                    reference_freq=reference_freq, tiling_utc=tiling_utc, tiling_method=tiling_method,
                    tiling_shape=tiling_shape, overlap=overlap, beam_shape_x=beam_shape_x,
                    beam_shape_y=beam_shape_y, beam_shape_angle=beam_shape_angle,
                    coordinate_type=coordinate_type
                )
                db_update = conn.execute(stmt)
                conn.commit()
                self.logger.debug(f"Added beam configuration")
                if return_id:
                    return db_update.lastrowid
            else:
                self.logger.debug(f"Beam configuration exists. Skipping...")
                if return_id:
                    return res[0]



    def _insert_beam_type(self, beam_type_name, description=None, return_id=False):
        table = self._get_table("beam_type")
        with self.engine.connect() as conn:
            stmt = select(table).where(table.c.name == beam_type_name).limit(1)
            res = conn.execute(stmt).first()
            if res is None:
                stmt = insert(table).values(name=beam_type_name, description=description)
                db_update = conn.execute(stmt)
                conn.commit()
                self.logger.debug(f"Added {beam_type_name} to beam_type table")
                if return_id:
                    return db_update.lastrowid
            else:
                self.logger.debug(f"{beam_type_name} exists. Skipping...")
                if return_id:
                    return res[0]

    def _insert_beam(self, beam_name, beam_ra_str, beam_dec_str, pointing_id, beam_type_id, tsamp_seconds, beam_config_id=None, is_coherent=1, return_id=False):
        table = self._get_table("beam")
        with self.engine.connect() as conn:
            stmt = (select(table.c.id)
                    .where(table.c.name == beam_name)
                    .where(table.c.pointing_id == pointing_id)
                    .where(table.c.beam_type_id == beam_type_id)
                    .where(table.c.beam_config_id == beam_config_id)
                    .limit(1))
            res = conn.execute(stmt).first()
            if res is None:
                stmt = insert(table).values(
                    name=beam_name, ra_str=beam_ra_str, dec_str=beam_dec_str,
                    pointing_id=pointing_id, beam_type_id=beam_type_id,
                    tsamp=tsamp_seconds, is_coherent=is_coherent, beam_config_id=beam_config_id
                )
                db_update = conn.execute(stmt)
                conn.commit()
                self.logger.debug(f"Added {beam_name} to beam table")
                if return_id:
                    return db_update.lastrowid
            else:
                self.logger.debug(f"{beam_name} in beam table. Skipping...")
                if return_id:
                    return res[0]

    def _insert_antenna(self, name, telescope_id, description=None, latitude_degrees=None, longitude_degrees=None, elevation_meters=None, return_id=False):
        table = self._get_table("antenna")
        with self.engine.connect() as conn:
            stmt = (select(table)
                    .where(table.c.name == name)
                    .where(table.c.telescope_id == telescope_id)
                    .limit(1))
            res = conn.execute(stmt).first()
            if res is None:
                stmt = insert(table).values(
                    name=name, description=description, telescope_id=telescope_id,
                    latitude_degrees=latitude_degrees, longitude_degrees=longitude_degrees,
                    elevation_meters=elevation_meters
                )
                db_update = conn.execute(stmt)
                conn.commit()
                self.logger.debug(f"Added {name} to antenna table")
                if return_id:
                    return db_update.lastrowid
            else:
                self.logger.debug(f"{name} exists in antenna table. Skipping...")
                if return_id:
                    return res[0]

    def _insert_beam_antenna(self, antenna_id, beam_id, description=None, return_id=False):
        table = self._get_table("beam_antenna")
        with self.engine.connect() as conn:
            stmt = (select(table)
                    .where(table.c.antenna_id == antenna_id)
                    .where(table.c.beam_id == beam_id)
                    .limit(1))
            res = conn.execute(stmt).first()
            if res is None:
                stmt = insert(table).values(antenna_id=antenna_id, beam_id=beam_id, description=description)
                db_update = conn.execute(stmt)
                conn.commit()
                self.logger.debug(f"Added beam_antenna link for beam_id {beam_id} and antenna_id {antenna_id}")
                if return_id:
                    return db_update.lastrowid
            else:
                self.logger.debug("beam_antenna link exists. Skipping...")
                if return_id:
                    return res[0]

    def _insert_file_type(self, file_type_name, description=None, return_id=False):
        table = self._get_table("file_type")
        with self.engine.connect() as conn:
            stmt = select(table).where(table.c.name == file_type_name).limit(1)
            res = conn.execute(stmt).first()
            if res is None:
                stmt = insert(table).values(name=file_type_name, description=description)
                db_update = conn.execute(stmt)
                conn.commit()
                self.logger.debug(f"Added {file_type_name} to file_type table")
                if return_id:
                    return db_update.lastrowid
            else:
                self.logger.debug(f"{file_type_name} in file_type table. Skipping...")
                if return_id:
                    return res[0]

    def _insert_data_product(self, beam_id, file_type_id, filename, filepath, available, locked, utc_start,
                             tsamp_seconds, tobs_seconds, nsamples, freq_start_mhz, freq_end_mhz, hardware_id,
                             nchans, nbits, filehash=None, metainfo=None, coherent_dm=None, subband_dm=None, fft_size=None, mjd_start=None, created_by_session_id=None, created_by_task_id=None, return_id=False):
        table = self._get_table("data_product")
        with self.engine.connect() as conn:
            stmt = (
                select(table)
                .where(table.c.file_type_id == file_type_id)
                .where(table.c.filepath == filepath)
                .where(table.c.filename == filename)
                .where(table.c.beam_id == beam_id)
                .where(table.c.hardware_id == hardware_id)
                .limit(1)
            )
            res = conn.execute(stmt).first()

            if res is None:
                new_id = UUIDUtility.generate_binary_uuid()
                stmt = insert(table).values(
                    id=new_id,
                    beam_id=beam_id,
                    file_type_id=file_type_id,
                    filename=filename,
                    filepath=filepath,
                    filehash=filehash,
                    available=available,
                    metainfo=metainfo,
                    locked=locked,
                    utc_start=utc_start,
                    tsamp=tsamp_seconds,
                    tobs=tobs_seconds,
                    nsamples=nsamples,
                    freq_start_mhz=freq_start_mhz,
                    freq_end_mhz=freq_end_mhz,
                    created_by_session_id=created_by_session_id,
                    created_by_task_id=created_by_task_id,
                    hardware_id=hardware_id,
                    fft_size=fft_size,
                    nchans=nchans,
                    nbits=nbits,
                    mjd_start=mjd_start,
                    coherent_dm=coherent_dm,
                    subband_dm=subband_dm
                )
               
                conn.execute(stmt)
                conn.commit()
                self.logger.debug(f"Added data product {filename} to data_product table")
                if return_id:
                    return UUIDUtility.convert_binary_uuid_to_string(new_id)
            else:
                self.logger.debug(f"Data product {filename} exists. Skipping...")
                if return_id:
                    return UUIDUtility.convert_binary_uuid_to_string(res[0])
    
    def _generate_argument_hash(self, params, keys_to_include):

        """
        Generates a SHA256 hash based on the specified keys in the parameters dictionary.

        :param params: Dictionary of parameters.
        :param keys_to_include: List of keys to include in the hash.
        :return: SHA256 hash string.
        """
        combined_args = "".join(str(params.get(key, "")) for key in keys_to_include)
        return hashlib.sha256(combined_args.encode()).hexdigest()

    def _generate_file_content_hash(self, file_path):
        """
        Generates a SHA256 hash for the given file.

        :param file_path: Path to the file.
        :return: SHA256 hash string.
        """
        hasher = hashlib.sha256()
        with open(file_path, "rb") as file:
            for chunk in iter(lambda: file.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _insert_pipeline(self, name, github_repo_name, github_commit_hash, github_branch, description=None, return_id = False):
        
        pipeline_table = self._get_table("pipeline")
        with self.engine.connect() as conn:
            
            stmt = (select(pipeline_table)
            .where(pipeline_table.c.name == name)
            .where(pipeline_table.c.github_repo_name == github_repo_name)
            .where(pipeline_table.c.github_commit_hash == github_commit_hash)
            .where(pipeline_table.c.github_branch == github_branch)
            )
            result = conn.execute(stmt).first()
        
            if result is None:
                stmt = insert(pipeline_table).values(
                    name=name,
                    github_repo_name=github_repo_name,
                    github_commit_hash=github_commit_hash,
                    github_branch=github_branch,
                    description=description
                )
                db_update = conn.execute(stmt)
                conn.commit()
                self.logger.debug(f"Added {name} to pipeline table")
                if return_id:
                    pipeline_id = db_update.lastrowid
                    return pipeline_id
            else:
                self.logger.debug(f"{name} exists in pipeline table. Skipping...")
                if return_id:
                    return result[0]
    
    def _insert_filtool(self, params, return_id=False):
    
        filtool_table = self._get_table("filtool")
        # Argument hash keys used for filtool. Modify if needed.
        filtool_keys = [
            'rfi_filter', 'zap_string', 'nbits', 'mean', 'std',
            'time_decimation_factor', 'freq_decimation_factor',
            'telescope_name', 'threads', 'extra_args', 'filplan'
        ]
        argument_hash = self._generate_argument_hash(params, filtool_keys)
        container_image_id = params['container_image_id']
        #Below are the keys that are not in the database, but are required for nextflow
        keys_to_exclude = ['get_metadata', 'program_name', 'container_image_path']

        # Handle default values and None conversions
        defaults = {
            'extra_args': None,
            'zap_string': None,
            'filplan': None,
            'time_decimation_factor': 1,
            'freq_decimation_factor': 1,
            'nbits': 8,
            'mean': 128,
            'std': 6
        }

        for key, default in defaults.items():
            params[key] = params.get(key, default)


        with self.engine.connect() as conn:
                        
            stmt = (
            select(filtool_table)
            .where(filtool_table.c.argument_hash == argument_hash)
            .where(filtool_table.c.container_image_id == container_image_id)
            .limit(1)
            )
            result = conn.execute(stmt).first()
            
            if result is None:
                #Add argument hash to params
                params['argument_hash'] = argument_hash
                #Exclude keys from params that are not in the database before inserting
                params = {k: v for k, v in params.items() if k not in keys_to_exclude}
                stmt = insert(filtool_table).values(params)
                db_update = conn.execute(stmt)
                conn.commit()
                filtool_id = db_update.inserted_primary_key[0]
                self.logger.debug(f"Added filtool parameters to filtool_params table")
                if return_id:
                    return filtool_id
            else:
                filtool_id = result[0]
                self.logger.debug(f"Filtool parameters exist. Skipping...")
                if return_id:
                    return filtool_id

    def _insert_peasoup(self, params, return_id=False):
        peasoup_table = self._get_table("peasoup")
        # Argument hash keys used for peasoup. Modify if needed.
        peasoup_keys = [
            'dm_start', 'dm_end', 'dm_step', 'coherent_dm', 'nh',
            'fft_size', 'start_sample', 'nsamples', 'acc_start', 'acc_end',
            'dm_pulse_width', 'acc_pulse_width', 'min_snr', 'ram_limit_gb',
            'total_cands', 'accel_tol', 'birdie_list', 'chan_mask', 'min_freq', 'max_freq'
            
        ]
        argument_hash = self._generate_argument_hash(params, peasoup_keys)
        params['argument_hash'] = argument_hash
        keys_to_exclude = ['program_name', 'container_image_path']
        with self.engine.connect() as conn:
            stmt = (
            select(peasoup_table)
            .where(peasoup_table.c.argument_hash == argument_hash)
            .limit(1)
            )
            result = conn.execute(stmt).first()
            if result is None:
                #Exclude keys from params that are not in the database before inserting
                params = {k: v for k, v in params.items() if k not in keys_to_exclude}
                stmt = insert(peasoup_table).values(params)
                db_update = conn.execute(stmt)
                conn.commit()
                peasoup_id = db_update.inserted_primary_key[0]
                self.logger.debug(f"Added peasoup parameters to peasoup_params table")
                if return_id:
                    return peasoup_id
            else:
                peasoup_id = result[0]
                self.logger.debug(f"Peasoup parameters exist. Skipping...")
                if return_id:
                    return peasoup_id

    def _insert_pulsarx(self, params, return_id=False):
        pulsarx_table = self._get_table("pulsarx")
        #Define keys for argument_hash if needed
        keys_for_hash = ['pepoch','nsubband','subint_length','start_frac','end_frac','clfd_q_value','fast_nbins','slow_nbins','rfi_filter','threads','extra_args']
        argument_hash = self._generate_argument_hash(params, keys_for_hash)
        params['argument_hash'] = argument_hash
        keys_to_exclude = ['program_name','container_image_path']

        with self.engine.connect() as conn:
            stmt = (
                select(pulsarx_table)
                .where(pulsarx_table.c.argument_hash == argument_hash)
                .limit(1)
            )
            result = conn.execute(stmt).first()
            if result is None:
                params_insert = {k: v for k, v in params.items() if k not in keys_to_exclude}
                stmt = insert(pulsarx_table).values(params_insert)
                db_update = conn.execute(stmt)
                conn.commit()
                pulsarx_id = db_update.inserted_primary_key[0]
                self.logger.debug(f"Added pulsarx parameters to pulsarx_params table")
                if return_id:
                    return pulsarx_id
            else:
                pulsarx_id = result[0]
                self.logger.debug(f"PulsarX parameters exist. Skipping...")
                if return_id:
                    return pulsarx_id

    def _insert_candidate_filter(self, params, return_id=False, generate_filehash=False):
        '''
        
        A unique candidate_filter should have:

        1. Unique Container Image ID (Hash from Dockerhub)
        2. Unique Name (program_name)
        3. Unique Filehash (if generate_filehash is True) or
        4. Unique combination of filename and filepath (if generate_filehash is False) and/or
        5. Unique extra_args (if present)

        '''
        candidate_filter_table = self._get_table("candidate_filter")

        # Map program name to name field in the database
        params['name'] = params['program_name']
        keys_to_exclude = ['program_name', 'container_image_path']

        with self.engine.connect() as conn:
            if generate_filehash:
                logging.info(f"Generating file hash for {params['filepath']}/{params['filename']}")
                filehash = self._generate_file_content_hash(f"{params['filepath']}/{params['filename']}")
                logging.info(f"File hash generated: {filehash}")
                params['filehash'] = filehash

                # Query to check if an entry with the same filehash exists and is associated with the same container image
                stmt = (
                    select(candidate_filter_table)
                    .where(candidate_filter_table.c.filehash == filehash)
                    .where(candidate_filter_table.c.container_image_id == params['container_image_id'])
                    .limit(1)
                )
            else:
                stmt = select(candidate_filter_table)
                # Check if filename and filepath exists and are not None
                if all(params.get(key) is not None for key in ['filename', 'filepath']):
                    # Combine filename and filepath for comparison
                    combined_path = f"{params['filepath']}/{params['filename']}"
                    stmt = stmt.where((candidate_filter_table.c.filepath + '/' + candidate_filter_table.c.filename) == combined_path)

                    # Check if extra_args is not null
                    if params.get('extra_args') is not None:

                        stmt = stmt.where(candidate_filter_table.c.extra_args == params['extra_args'])

                elif params.get('extra_args') is not None:
                    stmt = stmt.where(candidate_filter_table.c.extra_args == params['extra_args'])

                else:
                    # Fallback to checking uniqueness based on name
                    stmt = stmt.where(candidate_filter_table.c.name == params['name'])
                
                stmt = stmt.where(candidate_filter_table.c.container_image_id == params['container_image_id'])
                stmt = stmt.limit(1)
                

            # Execute the query
            result = conn.execute(stmt).first()

            if result is None:
                # Prepare parameters for insertion, excluding specific keys
                params_insert = {k: v for k, v in params.items() if k not in keys_to_exclude}
                stmt = insert(candidate_filter_table).values(params_insert)
                db_update = conn.execute(stmt)
                conn.commit()

                candidate_filter_id = db_update.inserted_primary_key[0]
                self.logger.debug("Added candidate filter parameters to candidate_filter table")

                if return_id:
                    return candidate_filter_id
            else:
                # Entry already exists
                candidate_filter_id = result[0]
                self.logger.debug("Candidate filter parameters exist. Skipping...")

                if return_id:
                    return candidate_filter_id




def main():
    parser = argparse.ArgumentParser(description="Upload data to database from a CSV")
    parser.add_argument('--project_name', default='COMPACT', help='Project Name')
    parser.add_argument('--telescope_name', default='MeerKAT', help='Telescope Name')
    parser.add_argument('--hardware_name', default='contra', help='This is applied to all files in your csv')
    parser.add_argument('--metadata_file', help='Project metadata file')
    parser.add_argument('--pointing_idx', type=int, help='Pointing index for BVRUSE metadata')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    parser.add_argument('--ra', help='Override RA')
    parser.add_argument('--dec', help='Override DEC')
    parser.add_argument('--antennas', nargs='+', help='Override antennas list')
    parser.add_argument('--target_name', help='Override target name')
    parser.add_argument('--freq_band', help='Override frequency band')
    parser.add_argument('--csv_file', required=True, help='Input CSV file')
    parser.add_argument('--nextflow_cfg', default="nf_config_for_data_upload.cfg", help='Flat Nextflow config file with pipeline input variables')
    parser.add_argument('--docker_hash_file', default="docker_image_digests.csv", help='Input file containing hash of all docker images used in the pipeline')


    args = parser.parse_args()

    # Load DB creds from env
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")

    overrides = {}
    if args.ra:
        overrides['ra'] = args.ra
    if args.dec:
        overrides['dec'] = args.dec
    if args.antennas:
        overrides['antennas'] = args.antennas
    if args.target_name:
        overrides['target_name'] = args.target_name
    if args.freq_band:
        overrides['freq_band'] = args.freq_band

    
    uploader = DatabaseUploader(
        db_host=DB_HOST,
        db_port=DB_PORT,
        db_username=DB_USERNAME,
        db_password=DB_PASSWORD,
        db_name=DB_NAME,
        project_name=args.project_name,
        telescope_name=args.telescope_name,
        hardware_name=args.hardware_name,
        project_metadata_file=args.metadata_file,
        pointing_idx=args.pointing_idx,
        docker_hash_file=args.docker_hash_file,
        nextflow_config_file=args.nextflow_cfg,
        verbose=args.verbose,
        overrides=overrides
    )

    uploader.upload_raw_data(args.csv_file)
    uploader.dump_lookup_table('file_type', 'lookup_tables/file_type.csv')
    uploader.dump_lookup_table('candidate_filter', 'lookup_tables/candidate_filter.csv')

if __name__ == "__main__":
    main()
