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
load_dotenv()


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

    def upload_from_csv(self, csv_file):
        """
        Read the CSV and upload all data products.
        CSV columns:
        filenames,beam_name,utc_start,hardware_name

        'filenames' may contain multiple files separated by spaces.
        """
        logging.info(f"IMPORTANT: Ensure Your nf_config_for_data_upload.cfg is updated by running nextflow config -profile contra -flat -sort > nf_config_for_data_upload.cfg")
        logging.info(f"Uploading data from {csv_file}")
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
        print(df)
        print(df.columns)
        
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
                #Insert beam_antenna linked table
                self._insert_beam_antenna(
                    antenna_id,
                    beam_id,
                    return_id=True
                )
            #Insert file type
            file_type_id = self._insert_file_type(
                beam_metadata['file_ext'],
                return_id=True
            )
            
            for filename in row['filenames'].split():
                dp_metadata = self._extract_file_header(filename)
                self._insert_data_product(
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
                    incoherent_dm=row['incoherent_dm'] if 'incoherent_dm' in row else None
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
        print(f"Pipeline ID: {pipeline_id}")
        docker_hash_df = pd.read_csv(self.docker_hash_file)
        print(docker_hash_df)

        sys.exit()

    
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

    def _prepare_program_parameters(self, params, docker_image_hashes):
        """
        Prepares program configurations for filtool and peasoup based on the provided nextflow configuration.
        This is used to get program_id(eg filtool_id, peasoup_id) with their corresponding arguments and docker hash for reproducibility.
        """
        filtool = {
            'program_name': 'filtool',
            'rfi_filter': params['filtool.rfi_filter'],
            'zap_string': params['filtool.zap_string'],
            'additional_flags': params['filtool.additional_flags'],
            'telescope': params['filtool.telescope'],
            'filplan': params['filtool.filplan'],
            'time_decimation_factor': params['filtool.time_decimation_factor'],
            'freq_decimation_factor': params['filtool.freq_decimation_factor'],
            'nbits': params['filtool.nbits'],
            'mean': params['filtool.mean'],
            'std': params['filtool.std'],
            'threads': params['filtool.threads'],
            'image_name': os.path.basename(params['apptainer_images.pulsarx']),
            'hash': docker_image_hashes.loc[docker_image_hashes['Image'] == 'pulsarx', 'SHA256'].values[0],
            'version': docker_image_hashes.loc[docker_image_hashes['Image'] == 'pulsarx', 'Version'].values[0],
            'container_type': "apptainer"
        }
        peasoup = {
            'program_name': 'peasoup',
            'fft_size': params['peasoup.fft_size'],
            'start_sample': params['peasoup.start_sample'],
            'nsamples': params['peasoup.nsamples'],
            'acc_start': params['peasoup.acc_start'],
            'acc_end': params['peasoup.acc_end'],
            'dm_pulse_width': params['peasoup.dm_pulse_width'],
            'acc_pulse_width': params['peasoup.acc_pulse_width'],
            'min_snr': params['peasoup.min_snr'],
            'ram_limit_gb': params['peasoup.ram_limit_gb'],
            'nh': params['peasoup.nh'],
            'ngpus': params['peasoup.ngpus'],
            'total_cands': params['peasoup.total_cands'],
            'accel_tol': params['peasoup.accel_tol'],
            'dm_file': params['peasoup.dm_file'],
            'image_name': os.path.basename(params['apptainer_images.peasoup']),
            'hash': docker_image_hashes.loc[docker_image_hashes['Image'] == 'peasoup', 'SHA256'].values[0],
            'version': docker_image_hashes.loc[docker_image_hashes['Image'] == 'peasoup', 'Version'].values[0],
            'container_type': "apptainer"
        }

        pulsarx = {
            'program_name': 'pulsarx',
            'subint_length': params['pulsarx.subint_length'],
            'clfd_q_value': params['pulsarx.clfd_q_value'],
            'rfi_filter': params['pulsarx.rfi_filter'],
            'fast_nbins': params['pulsarx.fast_nbins'],
            'slow_nbins': params['pulsarx.slow_nbins'],
            'nsubband': params['pulsarx.nsubband'],
            'fold_template': params['pulsarx.fold_template'],
            'threads': params['pulsarx.threads'],
            'image_name': os.path.basename(params['apptainer_images.pulsarx']),
            'hash': docker_image_hashes.loc[docker_image_hashes['Image'] == 'pulsarx', 'SHA256'].values[0],
            'version': docker_image_hashes.loc[docker_image_hashes['Image'] == 'pulsarx', 'Version'].values[0],
            'container_type': "apptainer"
        }

        prepfold = {
            'program_name': 'prepfold',
            'ncpus': params['prepfold.ncpus'],
            'mask': params['prepfold.mask'],
            'image_name': os.path.basename(params['apptainer_images.presto']),
            'hash': docker_image_hashes.loc[docker_image_hashes['Image'] == 'pulsar-miner', 'SHA256'].values[0],
            'version': docker_image_hashes.loc[docker_image_hashes['Image'] == 'pulsar-miner', 'Version'].values[0],
            'container_type': "apptainer"
        }
        return filtool, peasoup, pulsarx, prepfold

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
                             nchans, nbits, filehash=None, metainfo=None, coherent_dm=None, incoherent_dm=None, fft_size=None, mjd_start=None, created_by_processing_id=None):
        table = self._get_table("data_product")
        with self.engine.connect() as conn:
            stmt = (
                select(table)
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
                    created_by_processing_id=created_by_processing_id,
                    hardware_id=hardware_id,
                    fft_size=fft_size,
                    nchans=nchans,
                    nbits=nbits,
                    mjd_start=mjd_start,
                    coherent_dm=coherent_dm,
                    incoherent_dm=incoherent_dm
                )
               
                conn.execute(stmt)
                conn.commit()
                self.logger.debug(f"Added data product {filename} to data_product table")
            else:
                self.logger.debug(f"Data product {filename} exists. Skipping...")

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
                stmt = insert(pipeline_table).values(name=name, description=description, github_repo_name=github_repo_name, github_commit_hash=github_commit_hash, github_branch=github_branch)
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
    DBNAME = 'testdb'

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
        db_name=DBNAME,
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

    uploader.upload_from_csv(args.csv_file)

if __name__ == "__main__":
    main()
