import numpy as np
import pandas as pd
import sys
import argparse
import os
from dotenv import load_dotenv
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy import MetaData, Table, insert, select, text, func
from datetime import datetime
import hashlib
import decimal
# Load environment variables from .env file
load_dotenv()

# Postgres username, password, and database name
DB_SERVER = os.getenv("DB_HOST")  # Insert your DB address if it's not on Panoply
DB_PORT = os.getenv("DB_PORT")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")  # Change this to your Panoply/Postgres password
DBNAME = 'compact'  # Database name

connection_url = URL.create(
    "mysql+mysqlconnector", 
    username=DB_USERNAME, 
    password=DB_PASSWORD, 
    host=DB_SERVER, 
    database=DBNAME,
    port=DB_PORT
)
# Set up the engine and base
engine = create_engine(connection_url, echo=False)
metadata_obj = MetaData()
metadata_obj.reflect(bind=engine)

def get_table(table_name):
    return metadata_obj.tables[table_name]

#telescope_table = Table("telescope", metadata_obj, autoload_with=engine)
# telescope_table = get_table("telescope")
# project_table = get_table("project") 
# target_table = get_table("target")   
# pointing_table = get_table("pointing")
# beam_table = get_table("beam")
# beam_types_table = get_table("beam_types")
# antenna_table = get_table("antenna")
# hardware_table = get_table("hardware")
# peasoup_table = get_table("peasoup")
# pulsarx_table = get_table("pulsarx")
# filtool_table = get_table("filtool")
# prepfold_table = get_table("prepfold")
# processing_table = get_table("processing")
#observation_metadata = get_table("observation_metadata")

# stmt = select(target_table)
# stmt1 = select(telescope_table)
# stmt2 = select(project_table)
# stmt3 = select(pointing_table)
# stmt4 = select(antenna_table)
# stmt5 = select(processing_table)
#stmt1 = select(target_table)

#Delete all rows from the table
#stmt = telescope_table.delete()

# with engine.connect() as conn:
#     result = conn.execute(stmt5)
#     conn.commit()
#     for row in result:
#         print(row)

# with engine.connect() as conn:
#     result = conn.execute(stmt1)
#     for row in result:
#         print(row)

# with engine.connect() as conn:
#     result = conn.execute(stmt2)
#     for row in result:
#         print(row)

# with engine.connect() as conn:
#     result = conn.execute(stmt3)
#     for row in result:
#         print(row)

# with engine.connect() as conn:
#     result = conn.execute(stmt1)
#     for row in result:
#         print(row)


# #Orm method
# session = Session(engine)
# row = session.execute(select(telescope_table)).first()
# print(row)

def print_table(table_name):
    '''
    Print all rows in a given table
    '''
    table = get_table(table_name)
    with engine.connect() as conn:
        stmt = select(table)
        result = conn.execute(stmt)
        for row in result:
            print(row)

def delete_all_rows(table_name):
    
    table = get_table(table_name)
    with engine.connect() as conn:
        stmt = table.delete()
        conn.execute(stmt)
        conn.commit()
        print(f"Deleted all rows from {table_name} table")


def reset_primary_key_counter(table_name):

    with engine.connect() as conn:
        stmt = text(f"ALTER TABLE {table_name} AUTO_INCREMENT = 1")
        conn.execute(stmt)
        conn.commit()
        print(f"Reset primary key counter for {table_name} table")

def insert_telescope_name(telescope_name, telescope_description):

    telescope_table = get_table("telescope")
    with engine.connect() as conn:
        stmt = select(telescope_table).where(telescope_table.c.name == telescope_name).limit(1)
        result = conn.execute(stmt).first()
        
        if result is None:
            stmt = insert(telescope_table).values(name=telescope_name, description=telescope_description)
            conn.execute(stmt)
            conn.commit()
            print(f"Added {telescope_name} to telescope table")
        else:
            print(f"{telescope_name} already exists in telescope table. Skipping...")

def insert_project_name(project_name, project_description):

    project_table = get_table("project")
    with engine.connect() as conn:
        stmt = select(project_table).where(project_table.c.name == project_name).limit(1)
        result = conn.execute(stmt).first()
        
        if result is None:
            stmt = insert(project_table).values(name=project_name, description=project_description)
            conn.execute(stmt)
            conn.commit()
            print(f"Added {project_name} to project table")
        else:
            print(f"{project_name} already exists in project table. Skipping...")

def insert_candidate_filter_name(candidate_filter_name, candidate_filter_description):

    cand_filter_table = get_table("candidate_filter")
    with engine.connect() as conn:
        stmt = select(cand_filter_table).where(cand_filter_table.c.name == candidate_filter_name).limit(1)
        result = conn.execute(stmt).first()
        
        if result is None:
            stmt = insert(cand_filter_table).values(name=candidate_filter_name, description=candidate_filter_description)
            conn.execute(stmt)
            conn.commit()
            print(f"Added {candidate_filter_name} to candidate filter table")
        else:
            print(f"{candidate_filter_name} already exists in candidate filter table. Skipping...")

def insert_target_name(target_name, ra, dec, project_name, core_radius_arcmin_harris=None, core_radius_arcmin_baumgardt=None, half_mass_radius_arcmin_harris=None, half_mass_radius_arcmin_baumgardt=None, half_light_radius_arcmin_harris=None, half_light_radius_arcmin_baumgardt=None, description=None):
    
    '''
    Insert a new target into the target table if it doesn't already exist for the same project.
    A different ra and dec will trigger a new entry in the target table
    '''
   
    target_table = get_table("target")
    project_table = get_table("project")

    with engine.connect() as conn:
        #join target and project tables
        stmt = (select(target_table).join(project_table)
                .where(target_table.c.target_name == target_name)
                .where(target_table.c.ra == ra)
                .where(target_table.c.dec == dec)
                .where(project_table.c.name == project_name)
                .limit(1))
        result = conn.execute(stmt).first()
     
     
        if result is None:
            #get project id
            project_id = get_id_from_name("project", project_name)
            stmt = insert(target_table).values(target_name=target_name, ra=ra, dec=dec, notes=description, project_id=project_id, core_radius_arcmin_harris=core_radius_arcmin_harris, core_radius_arcmin_baumgardt=core_radius_arcmin_baumgardt, half_mass_radius_arcmin_harris=half_mass_radius_arcmin_harris, half_mass_radius_arcmin_baumgardt=half_mass_radius_arcmin_baumgardt, half_light_radius_arcmin_harris=half_light_radius_arcmin_harris, half_light_radius_arcmin_baumgardt=half_light_radius_arcmin_baumgardt)
            conn.execute(stmt)
            conn.commit()
            print(f"Added {target_name} to target table")
        else:
            print(f"{target_name} already exists with the given coordinates in target table for project {project_name}. Skipping...")




def insert_pointing(utc_start_str, tobs, nchans, freq_band, target_name, freq_start_mhz, freq_end_mhz, tsamp_seconds, telescope_name, receiver_name=None, return_id=False):
    '''
    Insert a new pointing into the pointing table if it doesn't already exist for the same target at the same UTC start time for the same project, telescope, and freq band.
    Optionally return the pointing_id of the inserted or existing row when return_id is True.
    '''

    pointing_table = get_table("pointing")
    target_table = get_table("target")
    telescope_table = get_table("telescope")
    
    utc_start = datetime.strptime(utc_start_str, '%Y-%m-%d-%H:%M:%S.%f')
    utc_start = utc_start.replace(microsecond=0)

    with engine.connect() as conn:
        stmt = (
            select(pointing_table.c.id)
            .join(target_table, target_table.c.id == pointing_table.c.target_id)
            .join(telescope_table, telescope_table.c.id == pointing_table.c.telescope_id)
            .where(target_table.c.target_name == target_name)
            .where(telescope_table.c.name == telescope_name)
            .where(pointing_table.c.utc_start == utc_start)
            .where(pointing_table.c.freq_band == freq_band)
            .limit(1)
        )

        result = conn.execute(stmt).first()

        if result is None:
            try:
                target_id = get_id_from_name("target", target_name, alternate_key='target_name')
            except:
                print(f"Target {target_name} does not exist in target table. Please add target first.")
                sys.exit()

            try:
                telescope_id = get_id_from_name("telescope", telescope_name)
            except:
                print(f"Telescope {telescope_name} does not exist in telescope table. Please add telescope first.")
                sys.exit()

            stmt = insert(pointing_table).values(
                utc_start=utc_start, tobs_seconds=tobs, nchan_raw=nchans, freq_band=freq_band,
                target_id=target_id, freq_start_mhz=freq_start_mhz, freq_end_mhz=freq_end_mhz,
                tsamp_raw_seconds=tsamp_seconds, telescope_id=telescope_id, receiver=receiver_name
            )
            result_proxy = conn.execute(stmt)
            conn.commit()
            print(f"Added pointing for {target_name} observed at {utc_start} with telescope {telescope_name} to pointing table.")


            if return_id:
                pointing_id = result_proxy.lastrowid  # For MariaDB/MySQL, use lastrowid to get the last inserted 'id'
                return pointing_id
            
        else:
            print(f"Pointing for {target_name} at {utc_start} already exists in pointing table. Skipping...")
            if return_id:
                return result[0]  # Assuming the first column selected is the id


def get_id_from_name(table_name, name, alternate_key=None):
    '''
    Get the id for a given name in a given table
    '''
    table = get_table(table_name)  

    with engine.connect() as conn:
        if alternate_key is not None:
            # Check if alternate key is a valid column name in the table
            if alternate_key not in table.c:
                print(f"Alternate key {alternate_key} not found in table {table_name}")
                sys.exit()
            stmt = select(table).where(getattr(table.c, alternate_key) == name).limit(1)
        else:
            # Assuming 'name' is a column in the table, and you are searching for a record with this column equal to the 'name' value
            stmt = select(table).where(table.c.name == name).limit(1)
        
        try:
            result = conn.execute(stmt).first()
            return result[0]
        
        except:
            print(f"{name} does not exist in {table_name} table. Please add {name} first.")
            sys.exit()
        


def insert_beam_type(beam_type_name, description=None):
    '''
    Insert a new beam type into the beam_type table if it doesn't already exist
    '''
    beam_type_table = get_table("beam_type")

    with engine.connect() as conn:
        
        stmt = select(beam_type_table).where(beam_type_table.c.name == beam_type_name).limit(1)
        result = conn.execute(stmt).first()
        
        if result is None:
            stmt = insert(beam_type_table).values(name=beam_type_name, description=description)
            conn.execute(stmt)
            conn.commit()
            print(f"Added {beam_type_name} to beam_type table")
        else:
            print(f"{beam_type_name} already exists in beam_type table. Skipping...")

def get_pointing_id(target_name, utc_start_str, project_name, telescope_name, freq_band):
    '''
    Get the pointing id for a given target, project, telescope and utc_start
    '''
    
    pointing_table = get_table("pointing")
    target_table = get_table("target")
    project_table = get_table("project")
    telescope_table = get_table("telescope")
   
    
    utc_start = datetime.strptime(utc_start_str, '%Y-%m-%d-%H:%M:%S.%f')
    utc_start = utc_start.replace(microsecond=0)

    with engine.connect() as conn:
        #join target and project tables
        stmt = (
            select(pointing_table.c.id)
            .join(target_table, target_table.c.id == pointing_table.c.target_id)
            .join(project_table, project_table.c.id == target_table.c.project_id)
            .join(telescope_table, telescope_table.c.id == pointing_table.c.telescope_id)
            .where(target_table.c.target_name == target_name)
            .where(project_table.c.name == project_name)
            .where(telescope_table.c.name == telescope_name)
            .where(pointing_table.c.utc_start == utc_start)
            .where(pointing_table.c.freq_band == freq_band)
            .limit(1)
        )
        result = conn.execute(stmt).first()
        if result is None:
            return None
        else:
            return result[0]

def get_beam_id(beam_name, beam_ra_str, beam_dec_str, pointing_id, beam_type_id):
    '''
    Get the beam id for a given beam name, pointing id and beam type id
    '''
    beam_table = get_table("beam")
    with engine.connect() as conn:
        stmt = select(beam_table).where(beam_table.c.name == beam_name).where(beam_table.c.pointing_id == pointing_id).where(beam_table.c.beam_type_id == beam_type_id).where(beam_table.c.ra_str == beam_ra_str).where(beam_table.c.dec_str == beam_dec_str).limit(1)
        result = conn.execute(stmt).first()
        if result is None:
            return None
        else:
            return result[0]
    


def insert_beam(beam_name, beam_ra_str, beam_dec_str, pointing_id, beam_type_id, tsamp_seconds, is_coherent=True, return_id=False):
    '''
    Insert a new beam into the beams table if it doesn't already exist
    '''

    beam_table = get_table("beam")

    with engine.connect() as conn:
        stmt = select(beam_table.c.id).where(beam_table.c.name == beam_name).where(beam_table.c.pointing_id == pointing_id).where(beam_table.c.beam_type_id == beam_type_id).limit(1)
        result = conn.execute(stmt).first()
        
        if result is None:
            stmt = insert(beam_table).values(name=beam_name, ra_str=beam_ra_str, dec_str=beam_dec_str, pointing_id=pointing_id, beam_type_id=beam_type_id, tsamp_seconds=tsamp_seconds, is_coherent=is_coherent)
            result_proxy = conn.execute(stmt)
            conn.commit()
            print(f"Added {beam_name} to beam table")
            if return_id:
                beam_id = result_proxy.lastrowid
                return beam_id
        else:
            print(f"{beam_name} already exists in beam table. Skipping...")
            if return_id:
                return result[0]  # Assuming the first column selected is the id


def insert_beam_without_pointing_id(beam_name, beam_ra_str, beam_dec_str, beam_type_name, utc_start_str, project_name, telescope_name, target_name, freq_band, tsamp_seconds, is_coherent=True, return_id=False):
    '''
    Insert a new beam into the beams table if it doesn't already exist.
    A combination of unique utc_start, project_name, telescope_name, freq_band, and target_name identifies a unique pointing.
    '''

    beam_table = get_table("beam")
    # Assuming get_pointing_id and get_id_from_name are utility functions that you've defined elsewhere
    utc_start = datetime.strptime(utc_start_str, '%Y-%m-%d-%H:%M:%S.%f')
    utc_start = utc_start.replace(microsecond=0)

    # Get Pointing ID
    pointing_id = get_pointing_id(target_name, utc_start_str, project_name, telescope_name, freq_band)

    # Get beam type ID
    beam_type_id = get_id_from_name("beam_type", beam_type_name)
  
    with engine.connect() as conn:
        stmt = select(beam_table.c.id).where(beam_table.c.name == beam_name).where(beam_table.c.pointing_id == pointing_id).where(beam_table.c.beam_type_id == beam_type_id).limit(1)
        result = conn.execute(stmt).first()
       
        if result is None:
            stmt = insert(beam_table).values(name=beam_name, ra_str=beam_ra_str, dec_str=beam_dec_str, pointing_id=pointing_id, beam_type_id=beam_type_id, tsamp_seconds=tsamp_seconds, is_coherent=is_coherent)
            result_proxy = conn.execute(stmt)
            conn.commit()
            print(f"Added {beam_name} to beam table")
            if return_id:
                beam_id = result_proxy.lastrowid
                return beam_id
        else:
            print(f"{beam_name} already exists in beam table. Skipping...")
            if return_id:
                return result[0]  # Assuming the first column selected is the id


def insert_file_type(file_type_name, description=None):
    '''
    Insert a new file type into the file_types table if it doesn't already exist
    '''
    
    file_type_table = get_table("file_type")
    with engine.connect() as conn:
        
        stmt = select(file_type_table).where(file_type_table.c.name == file_type_name).limit(1)
        result = conn.execute(stmt).first()
        
        if result is None:
            stmt = insert(file_type_table).values(name=file_type_name, description=description)
            conn.execute(stmt)
            conn.commit()
            print(f"Added {file_type_name} to file_type table")
        else:
            print(f"{file_type_name} already exists in file_type table. Skipping...")

def insert_antenna(name, telescope_name, latitude_degrees=None, longitude_degrees=None, elevation_meters=None, description=None):
    '''
    Insert a new antenna into the antenna table if it doesn't already exist for the same telescope
    '''
    
    antenna_table = get_table("antenna")
    telescope_table = get_table("telescope")
    with engine.connect() as conn:
            
            stmt = (
                select(antenna_table.c.id)
                .join(telescope_table, telescope_table.c.id == antenna_table.c.telescope_id)
                .where(antenna_table.c.name == name)
                .where(telescope_table.c.name == telescope_name)
                .limit(1)
            )
            result = conn.execute(stmt).first()
            
            if result is None:
                telescope_id = get_id_from_name("telescope", telescope_name)
                stmt = insert(antenna_table).values(name=name, description=description, telescope_id=telescope_id, latitude_degrees=latitude_degrees, longitude_degrees=longitude_degrees, elevation_meters=elevation_meters)
                conn.execute(stmt)
                conn.commit()
                print(f"Added {name} to antenna table")
            else:
                print(f"{name} already exists in antenna table for telescope {telescope_name}. Skipping...")

def insert_hardware(hardware_name, job_scheduler, hardware_description=None):
    '''
    Insert a new hardware into the hardware table if it doesn't already exist
    '''
    
    hardware_table = get_table("hardware")
    with engine.connect() as conn:
        
        stmt = select(hardware_table).where(hardware_table.c.name == hardware_name).limit(1)
        result = conn.execute(stmt).first()
        
        if result is None:
            stmt = insert(hardware_table).values(name=hardware_name, description=hardware_description, job_scheduler=job_scheduler)
            conn.execute(stmt)
            conn.commit()
            print(f"Added {hardware_name} to hardware table")
        else:
            print(f"{hardware_name} already exists in hardware table. Skipping...")

def insert_pipeline(name, github_repo_name, github_commit_hash, github_branch, description=None):
    '''
    Insert a new pipeline into the pipeline table if it doesn't already exist
    '''
    
    pipeline_table = get_table("pipeline")
    with engine.connect() as conn:
        
        stmt = (select(pipeline_table)
        .where(pipeline_table.c.name == name)
        .where(pipeline_table.c.github_repo_name == github_repo_name)
        .where(pipeline_table.c.github_commit_hash == github_commit_hash)
        .where(pipeline_table.c.github_branch == github_branch)
         )
        result = conn.execute(stmt).first()
       
        if result is None:
            stmt = insert(pipeline_table).values(name=name, description=description, github_repo_name=github_repo_name, github_commit_hash=github_commit_hash, github_branch=github_branch)
            conn.execute(stmt)
            conn.commit()
            print(f"Added {name} to pipeline table")
        else:
            print(f"{name} already exists in pipeline table. Skipping...")


def insert_pipeline_execution_order(pipeline_id, program_name, execution_order, peasoup_id=None, pulsarx_id=None, filtool_id=None, prepfold_id=None, circular_orbit_search_id=None, elliptical_orbit_search_id=None, rfifind_id=None):
    '''
    Insert a new pipeline execution order into the pipeline_execution_order table if it doesn't already exist
    '''
    
    pipeline_execution_order_table = get_table("pipeline_execution_order")
    with engine.connect() as conn:
        
        stmt = (select(pipeline_execution_order_table)
        .where(pipeline_execution_order_table.c.pipeline_id == pipeline_id)
        .where(pipeline_execution_order_table.c.program_name == program_name)
        .where(pipeline_execution_order_table.c.execution_order == execution_order)
        .where(pipeline_execution_order_table.c.peasoup_id == peasoup_id)
        .where(pipeline_execution_order_table.c.pulsarx_id == pulsarx_id)
        .where(pipeline_execution_order_table.c.filtool_id == filtool_id)
        .where(pipeline_execution_order_table.c.prepfold_id == prepfold_id)
        .where(pipeline_execution_order_table.c.circular_orbit_search_id == circular_orbit_search_id)
        .where(pipeline_execution_order_table.c.elliptical_orbit_search_id == elliptical_orbit_search_id)
        .where(pipeline_execution_order_table.c.rfifind_id == rfifind_id)
        )
        result = conn.execute(stmt).first()
        
        if result is None:
            stmt = insert(pipeline_execution_order_table).values(pipeline_id=pipeline_id, program_name=program_name, execution_order=execution_order, peasoup_id=peasoup_id, pulsarx_id=pulsarx_id, filtool_id=filtool_id, prepfold_id=prepfold_id, circular_orbit_search_id=circular_orbit_search_id, elliptical_orbit_search_id=elliptical_orbit_search_id, rfifind_id=rfifind_id)
            conn.execute(stmt)
            conn.commit()
            print(f"Added {program_name} to pipeline_execution_order table")
        else:
            print(f"{program_name} already exists in pipeline_execution_order table. Skipping...")

def insert_peasoup(acc_start, acc_end, min_snr, ram_limit_gb, nharmonics, ngpus, total_cands_limit, fft_size, dm_file, accel_tol, birdie_list, chan_mask, extra_args, container_image_name, container_image_version, container_type, container_image_id, pipeline_github_commit_hash, execution_order, argument_hash=None):
    ''' Insert Peasoup parameters into the peasoup_params table if it doesn't already exist ''' 
    peasoup_table = get_table("peasoup")
    combined_args = f"{acc_start}{acc_end}{min_snr}{ram_limit_gb}{nharmonics}{ngpus}{total_cands_limit}{fft_size}{dm_file}{accel_tol}{birdie_list}{chan_mask}{extra_args}"
    # Generate SHA256 hash
    argument_hash = hashlib.sha256(combined_args.encode()).hexdigest()
    #Get pipeline id to add in execution order table
    pipeline_id = get_id_from_name("pipeline", pipeline_github_commit_hash, alternate_key='github_commit_hash')
   
    with engine.connect() as conn:
            
        stmt = (
        select(peasoup_table)
        .where(peasoup_table.c.argument_hash == argument_hash)
        .where(peasoup_table.c.container_image_id == container_image_id)
        .limit(1)
        )
        result = conn.execute(stmt).first()
        
        if result is None:
            
            stmt = insert(peasoup_table).values(acc_start=acc_start, acc_end=acc_end, min_snr=min_snr, ram_limit_gb=ram_limit_gb, nharmonics=nharmonics, ngpus=ngpus, total_cands_limit=total_cands_limit, fft_size=fft_size, dm_file=dm_file, accel_tol=accel_tol, birdie_list=birdie_list, chan_mask=chan_mask, extra_args=extra_args, container_image_name=container_image_name, container_image_version=container_image_version, container_type=container_type, container_image_id=container_image_id, argument_hash=argument_hash)
            db_update = conn.execute(stmt)
            conn.commit()
            peasoup_id = db_update.inserted_primary_key[0]
            print(f"Added Peasoup parameters to peasoup_params table")
            
            
        else:
            peasoup_id = result[0]
            print(f"Peasoup parameters already exist in peasoup_params table. Skipping...")
    #Add peasoup to pipeline execution order table
    print("Peasoup ID: ", peasoup_id)
    #insert_pipeline_execution_order(pipeline_id, "peasoup", execution_order, peasoup_id=peasoup_id)

            

def insert_pulsarx(subbands_number, subint_length, clfd_q_value, fast_period_bins, slow_period_bins, rfi_filter, extra_args, threads, container_image_name, container_image_version, container_type, container_image_id, pipeline_github_commit_hash, execution_order, argument_hash=None):
    '''
    Insert PulsarX parameters into the pulsarx_params table if it doesn't already exist
    '''
    pulsarx_table = get_table("pulsarx")
    combined_args = f"{subbands_number}{subint_length}{clfd_q_value}{fast_period_bins}{slow_period_bins}{rfi_filter}{extra_args}{threads}"
    # Generate SHA256 hash
    argument_hash = hashlib.sha256(combined_args.encode()).hexdigest()
    #Get pipeline id to add in execution order table
    pipeline_id = get_id_from_name("pipeline", pipeline_github_commit_hash, alternate_key='github_commit_hash')

    with engine.connect() as conn:
                
        stmt = (
        select(pulsarx_table)
        .where(pulsarx_table.c.argument_hash == argument_hash)
        .where(pulsarx_table.c.container_image_id == container_image_id)
        .limit(1)
        )
        result = conn.execute(stmt).first()
        
        if result is None:
            stmt = insert(pulsarx_table).values(subbands_number=subbands_number, subint_length=subint_length, clfd_q_value=clfd_q_value, fast_nbins=fast_period_bins, slow_nbins=slow_period_bins, rfi_filter=rfi_filter, extra_args=extra_args, threads=threads, container_image_name=container_image_name, container_image_version=container_image_version, container_type=container_type, container_image_id=container_image_id, argument_hash=argument_hash)
            db_update = conn.execute(stmt)
            conn.commit()
            pulsarx_id = db_update.inserted_primary_key[0]
            print(f"Added PulsarX parameters to pulsarx_params table")
        else:
            pulsarx_id = result[0]
            print(f"PulsarX parameters already exist in pulsarx_params table. Skipping...")
    #Add pulsarx to pipeline execution order table
    #insert_pipeline_execution_order(pipeline_id, "pulsarx", execution_order, pulsarx_id=pulsarx_id)

def insert_filtool(rfi_filter, telescope_name, threads, extra_args, container_image, container_version, container_type, container_image_id, pipeline_github_commit_hash, execution_order, argument_hash=None):
    '''
    Insert Filtool parameters into the filtool_params table if it doesn't already exist
    '''
    filtool_table = get_table("filtool")
    combined_args = f"{rfi_filter}{telescope_name}{threads}{extra_args}"
    # Generate SHA256 hash
    argument_hash = hashlib.sha256(combined_args.encode()).hexdigest()
    #Get pipeline id to add in execution order table
    pipeline_id = get_id_from_name("pipeline", pipeline_github_commit_hash, alternate_key='github_commit_hash')

    with engine.connect() as conn:
                    
        stmt = (
        select(filtool_table)
        .where(filtool_table.c.argument_hash == argument_hash)
        .where(filtool_table.c.container_image_id == container_image_id)
        .limit(1)
        )
        result = conn.execute(stmt).first()
        
        if result is None:
            stmt = insert(filtool_table).values(rfi_filter=rfi_filter, telescope_name=telescope_name, threads=threads, extra_args=extra_args, container_image_name=container_image, container_image_version=container_version, container_type=container_type, container_image_id=container_image_id, argument_hash=argument_hash)
            db_update = conn.execute(stmt)
            conn.commit()
            filtool_id = db_update.inserted_primary_key[0]
            print(f"Added Filtool parameters to filtool_params table")
        else:
            filtool_id = result[0]
            print(f"Filtool parameters already exist in filtool_params table. Skipping...")
    #Add filtool to pipeline execution order table
    #insert_pipeline_execution_order(pipeline_id, "filtool", execution_order, filtool_id=filtool_id)

def insert_prepfold(ncpus, rfifind_mask, extra_args, container_image, container_version, container_type, container_image_id, pipeline_github_commit_hash, execution_order, argument_hash=None):
    '''
    Insert Prepfold parameters into the prepfold_params table if it doesn't already exist
    '''
    prepfold_table = get_table("prepfold")
    combined_args = f"{ncpus}{extra_args}"
    # Generate SHA256 hash
    argument_hash = hashlib.sha256(combined_args.encode()).hexdigest()
    #Get pipeline id to add in execution order table
    pipeline_id = get_id_from_name("pipeline", pipeline_github_commit_hash, alternate_key='github_commit_hash')

    with engine.connect() as conn:
                            
        stmt = (
        select(prepfold_table)
        .where(prepfold_table.c.argument_hash == argument_hash)
        .where(prepfold_table.c.container_image_id == container_image_id)
        .limit(1)
        )
        result = conn.execute(stmt).first()
        
        if result is None:
            stmt = insert(prepfold_table).values(ncpus=ncpus, rfifind_mask=rfifind_mask, extra_args=extra_args, container_image_name=container_image, container_image_version=container_version, container_type=container_type, container_image_id=container_image_id, argument_hash=argument_hash)
            db_update = conn.execute(stmt)
            conn.commit()
            prepfold_id = db_update.inserted_primary_key[0]
            print(f"Added Prepfold parameters to prepfold_params table")
        else:
            prepfold_id = result[0]
            print(f"Prepfold parameters already exist in prepfold_params table. Skipping...")
    #Add prepfold to pipeline execution order table
    #insert_pipeline_execution_order(pipeline_id, "prepfold", execution_order, prepfold_id=prepfold_id)

def insert_circular_orbit_search(min_porb_h, max_porb_h, min_pulsar_mass_m0, max_comp_mass_m0, min_orb_phase_rad, max_orb_phase_rad, coverage, mismatch, container_image_name, container_image_version, container_type, container_image_id, pipeline_github_commit_hash, execution_order, argument_hash=None):
    '''
    Insert Circular Orbit Search parameters into the circular_orbit_search_params table if it doesn't already exist
    '''
    circular_orbit_search_table = get_table("circular_orbit_search")
    combined_args = f"{min_porb_h}{max_porb_h}{min_pulsar_mass_m0}{max_comp_mass_m0}{min_orb_phase_rad}{max_orb_phase_rad}{coverage}{mismatch}"
    # Generate SHA256 hash
    argument_hash = hashlib.sha256(combined_args.encode()).hexdigest()
    #Get pipeline id to add in execution order table
    pipeline_id = get_id_from_name("pipeline", pipeline_github_commit_hash, alternate_key='github_commit_hash')

    with engine.connect() as conn:
                                
        stmt = (
        select(circular_orbit_search_table)
        .where(circular_orbit_search_table.c.argument_hash == argument_hash)
        .where(circular_orbit_search_table.c.container_image_id == container_image_id)
        .limit(1)
        )
        result = conn.execute(stmt).first()
        
        if result is None:
            stmt = insert(circular_orbit_search_table).values(min_porb_h=min_porb_h, max_porb_h=max_porb_h, min_pulsar_mass_m0=min_pulsar_mass_m0, max_comp_mass_m0=max_comp_mass_m0, min_orb_phase_rad=min_orb_phase_rad, max_orb_phase_rad=max_orb_phase_rad, coverage=coverage, mismatch=mismatch, container_image_name=container_image_name, container_image_version=container_image_version, container_type=container_type, container_image_id=container_image_id, argument_hash=argument_hash)
            db_update = conn.execute(stmt)
            conn.commit()
            circular_orbit_search_id = db_update.inserted_primary_key[0]
            print(f"Added Circular Orbit Search parameters to circular_orbit_search_params table")
        else:
            circular_orbit_search_id = result[0]
            print(f"Circular Orbit Search parameters already exist in circular_orbit_search_params table. Skipping...")
    #Add circular orbit search to pipeline execution order table
    #insert_pipeline_execution_order(pipeline_id, "circular_orbit_search", execution_order, circular_orbit_search_id=circular_orbit_search_id)

def insert_elliptical_orbit_search(min_porb_h, max_porb_h, min_pulsar_mass_m0, max_comp_mass_m0, min_orb_phase_rad, max_orb_phase_rad, min_ecc, max_ecc, min_periastron_rad, max_periastron_rad, coverage, mismatch, container_image_name, container_image_version, container_type, container_image_id, pipeline_github_commit_hash, execution_order, argument_hash=None):
    '''
    Insert Elliptical Orbit Search parameters into the elliptical_orbit_search_params table if it doesn't already exist
    '''
    elliptical_orbit_search_table = get_table("elliptical_orbit_search")
    combined_args = f"{min_porb_h}{max_porb_h}{min_pulsar_mass_m0}{max_comp_mass_m0}{min_orb_phase_rad}{max_orb_phase_rad}{min_ecc}{max_ecc}{min_periastron_rad}{max_periastron_rad}{coverage}{mismatch}"
    # Generate SHA256 hash
    argument_hash = hashlib.sha256(combined_args.encode()).hexdigest()
    #Get pipeline id to add in execution order table
    pipeline_id = get_id_from_name("pipeline", pipeline_github_commit_hash, alternate_key='github_commit_hash')

    with engine.connect() as conn:
                                    
        stmt = (
        select(elliptical_orbit_search_table)
        .where(elliptical_orbit_search_table.c.argument_hash == argument_hash)
        .where(elliptical_orbit_search_table.c.container_image_id == container_image_id)
        .limit(1)
        )
        result = conn.execute(stmt).first()
        
        if result is None:
            stmt = insert(elliptical_orbit_search_table).values(min_porb_h=min_porb_h, max_porb_h=max_porb_h, min_pulsar_mass_m0=min_pulsar_mass_m0, max_comp_mass_m0=max_comp_mass_m0, min_orb_phase_rad=min_orb_phase_rad, max_orb_phase_rad=max_orb_phase_rad, min_ecc=min_ecc, max_ecc=max_ecc, min_periastron_rad=min_periastron_rad, max_periastron_rad=max_periastron_rad, coverage=coverage, mismatch=mismatch, container_image_name=container_image_name, container_image_version=container_image_version, container_type=container_type, container_image_id=container_image_id, argument_hash=argument_hash)
            db_update = conn.execute(stmt)
            conn.commit()
            elliptical_orbit_search_id = db_update.inserted_primary_key[0]
            print(f"Added Elliptical Orbit Search parameters to elliptical_orbit_search_params table")
        else:
            elliptical_orbit_search_id = result[0]
            print(f"Elliptical Orbit Search parameters already exist in elliptical_orbit_search_params table. Skipping...")
    #Add elliptical orbit search to pipeline execution order table
    #insert_pipeline_execution_order(pipeline_id, "elliptical_orbit_search", execution_order, elliptical_orbit_search_id=elliptical_orbit_search_id)

def insert_rfifind(time, time_sigma, freq_sigma, chan_frac, int_frac, ncpus, extra_args, container_image_name, container_image_version, container_type, container_image_id, pipeline_github_commit_hash, execution_order, argument_hash=None):
    '''
    Insert RFIfind parameters into the rfifind_params table if it doesn't already exist
    '''
    rfifind_table = get_table("rfifind")
    combined_args = f"{time}{time_sigma}{freq_sigma}{chan_frac}{int_frac}{ncpus}{extra_args}"
    # Generate SHA256 hash
    argument_hash = hashlib.sha256(combined_args.encode()).hexdigest()
    #Get pipeline id to add in execution order table
    pipeline_id = get_id_from_name("pipeline", pipeline_github_commit_hash, alternate_key='github_commit_hash')

    with engine.connect() as conn:
                                        
        stmt = (
        select(rfifind_table)
        .where(rfifind_table.c.argument_hash == argument_hash)
        .where(rfifind_table.c.container_image_id == container_image_id)
        .limit(1)
        )
        result = conn.execute(stmt).first()
        
        if result is None:
            stmt = insert(rfifind_table).values(time=time, time_sigma=time_sigma, freq_sigma=freq_sigma, chan_frac=chan_frac, int_frac=int_frac, ncpus=ncpus, extra_args=extra_args, container_image_name=container_image_name, container_image_version=container_image_version, container_type=container_type, container_image_id=container_image_id, argument_hash=argument_hash)
            db_update = conn.execute(stmt)
            conn.commit()
            rfifind_id = db_update.inserted_primary_key[0]
            print(f"Added RFIfind parameters to rfifind_params table")
        else:
            rfifind_id = result[0]
            print(f"RFIfind parameters already exist in rfifind_params table. Skipping...")
    #Add rfifind to pipeline execution order table
    #insert_pipeline_execution_order(pipeline_id, "rfifind", execution_order, rfifind_id=rfifind_id)

    #insert_processing(2, "123456", "Hercules", "2021-01-30-11:54:02.05986", "SUBMITTED", 1, 3, 1, "Peasoup", "123456") 

def insert_processing(data_product_ids, pipeline_github_commit_hash, hardware_name, submit_time, process_status, attempt_number, max_attempts, execution_order, program_name, argument_hash, start_time=None, end_time=None, return_id=False):
    '''
    Insert or update a processing entry in the processing table if it doesn't already exist for the same data product, pipeline, hardware, and argument hash.
    Now supports processing for multiple data_product_ids.
    '''
    processing_table = get_table("processing")
    pipeline_table = get_table("pipeline")
    hardware_table = get_table("hardware")
    program_table = get_table(program_name)
    foreign_column = f"{program_name}_id"
    data_product_table = get_table("data_product")
    processing_dp_table = get_table("processing_dp_inputs")
    
    with engine.connect() as conn:
        # Check if the process with the same argument hash on the given data_product (first in list) is running on any hardware
        stmt = (
            select(processing_table)
            .join(pipeline_table, pipeline_table.c.id == processing_table.c.pipeline_id)
            .join(hardware_table, hardware_table.c.id == processing_table.c.hardware_id)
            .join(program_table, program_table.c.id == getattr(processing_table.c, foreign_column))
            .join(processing_dp_table, processing_table.c.id == processing_dp_table.c.processing_id)
            .join(data_product_table, processing_dp_table.c.dp_id == data_product_table.c.id)
            .where(pipeline_table.c.github_commit_hash == pipeline_github_commit_hash)
            .where(program_table.c.argument_hash == argument_hash)
            .where(processing_table.c.program_name == program_name)
            .where(processing_dp_table.c.dp_id == data_product_ids[0])  # Only check the first data_product_id
            .limit(1)
        )
        result = conn.execute(stmt).first()
        
        if result is None:
            # Insert new processing if it doesn't exist for the first data_product_id
            pipeline_id = get_id_from_name("pipeline", pipeline_github_commit_hash, alternate_key='github_commit_hash')
            hardware_id = get_id_from_name("hardware", hardware_name)
            program_id = get_id_from_name(program_name, argument_hash, alternate_key='argument_hash')
            
            insert_values = {
                "pipeline_id": pipeline_id,
                "hardware_id": hardware_id,
                "submit_time": submit_time,
                "process_status": process_status,
                "attempt_number": attempt_number,
                "max_attempts": max_attempts,
                "start_time": start_time,
                "end_time": end_time,
                "execution_order": execution_order,
                "program_name": program_name,
                foreign_column: program_id
            }
            stmt = insert(processing_table).values(**insert_values)
            result_proxy = conn.execute(stmt)
            processing_id = result_proxy.lastrowid
            conn.commit()
            print(f"Added processing to processing table")
            
            # Insert into processing_dp_inputs table for each data_product_id
            for data_product_id in data_product_ids:
                stmt = insert(processing_dp_table).values(processing_id=processing_id, dp_id=data_product_id)
                conn.execute(stmt)
            conn.commit()
            
            if return_id:
                return processing_id
        else:
            processing_id = result[0]  # Assuming first column is the ID
            # Check if there's a status update
            if result.process_status != process_status or result.attempt_number != attempt_number:
                # Update processing status, start_time, end_time, and attempt_number
                stmt = (
                    processing_table.update()
                    .where(processing_table.c.id == processing_id)
                    .values(process_status=process_status, start_time=start_time, end_time=end_time, attempt_number=attempt_number)
                )
                conn.execute(stmt)
                conn.commit()
                print(f"Updated status of processing with id {processing_id} to {process_status}")
            else:
                print(f"Processing already exists in processing table. Skipping...")
            
            # Insert into processing_dp_inputs table for each data_product_id if not already linked. Extra Check
            for data_product_id in data_product_ids:
                # Check if the processing_id and data_product_id link already exists
                check_stmt = select(processing_dp_table).where(
                    processing_dp_table.c.processing_id == processing_id,
                    processing_dp_table.c.dp_id == data_product_id
                )
                link_exists = conn.execute(check_stmt).first()
                if not link_exists:
                    stmt = insert(processing_dp_table).values(processing_id=processing_id, dp_id=data_product_id)
                    conn.execute(stmt)
            conn.commit()
            if return_id:
                return processing_id
            
                                


def insert_data_product(beam_id, file_type_name, filename, filepath, filehash, available, upload_date, modification_date, metainfo, locked, utc_start, tsamp_seconds, tobs_seconds, nsamples, freq_start_mhz, freq_end_mhz, created_by_processing_id, hardware_name, hash_check=False, return_id=False, fft_size=None, tstart=None):
    '''
    Insert a new data product into the data_product table if it doesn't already exist
    Checks if a data product with the same filepath and filename exists or, optionally, if hash_check is True, checks by filehash.
    Optional feature to be added later. Join with beam table and check for same beam id before inserting.
    '''
    data_product_table = get_table("data_product")

    with engine.connect() as conn:
        if hash_check:
            # Check if a data product with the same filehash exists
            stmt = (
                select(data_product_table)
                .where(data_product_table.c.filehash == filehash)
                .limit(1)
            )
        else:
            # Check if a data product with the same filepath and filename exists
            full_path = filepath + filename  # Concatenate filepath and filename
            stmt = (
                select(data_product_table)
                .where(data_product_table.c.filepath + data_product_table.c.filename == full_path)
                .where(data_product_table.c.beam_id == beam_id)
                .limit(1)
            )

        result = conn.execute(stmt).first()
        if result is None:
            # Get file type id
            file_type_id = get_id_from_name("file_type", file_type_name)
            # Get hardware id
            hardware_id = get_id_from_name("hardware", hardware_name)

            stmt = insert(data_product_table).values(
                beam_id=beam_id,
                file_type_id=file_type_id,
                filename=filename,
                filepath=filepath,
                filehash=filehash,
                available=available,
                upload_date=upload_date,
                modification_date=modification_date,
                metainfo=metainfo,
                locked=locked,
                utc_start=utc_start,
                tsamp_seconds=tsamp_seconds,
                tobs_seconds=tobs_seconds,
                nsamples=nsamples,
                freq_start_mhz=freq_start_mhz,
                freq_end_mhz=freq_end_mhz,
                created_by_processing_id=created_by_processing_id,
                hardware_id=hardware_id,
                fft_size=fft_size,
                tstart=tstart
            )
            result_proxy = conn.execute(stmt)
            conn.commit()
            print(f"Added data product to data_product table")
            if return_id:
                data_product_id = result_proxy.lastrowid
                return data_product_id
        else:
            print(f"Data product already exists in data_product table. Skipping...")
            if return_id:
                return result[0]


def insert_search_candidate(pointing_id, beam_id, processing_id, spin_period, dm, snr, filename, filepath, nh, dp_id, candidate_id_in_file, pdot=None, pdotdot=None, pb=None, x=None, t0=None, omega=None, e=None, ddm_count_ratio=None, ddm_snr_ratio=None, nassoc=None, metadata_hash=None, candidate_filter_id=None):
    '''
    Insert a new search_candidate into search_candidate. No unique check as there will be millions of candidates.
    
    '''
    search_candidate_table = get_table("search_candidate")
    # combined_args = f"{pointing_id}{beam_id}{processing_id}{spin_period}{dm}{snr}{tstart}{nh}{dp_id}{candidate_id_in_file}{pdot}{pdotdot}{pb}{x}{t0}{omega}{e}"
    # # Generate SHA256 hash
    # metadata_hash = hashlib.sha256(combined_args.encode()).hexdigest()

    
    with engine.connect() as conn:
        stmt = insert(search_candidate_table).values(pointing_id=pointing_id, beam_id=beam_id, processing_id=processing_id, spin_period=spin_period, dm=dm, snr=snr, filename=filename, filepath=filepath, nh=nh, metadata_hash=metadata_hash, candidate_filter_id=candidate_filter_id, dp_id=dp_id, candidate_id_in_file=candidate_id_in_file, pdot=pdot, pdotdot=pdotdot, pb=pb, x=x, t0=t0, omega=omega, e=e, ddm_count_ratio=ddm_count_ratio, ddm_snr_ratio=ddm_snr_ratio, nassoc=nassoc)
        conn.execute(stmt)
        conn.commit()
        print(f"Added search candidate to search_candidate table")

def insert_fold_candidate(pointing_id, beam_id, processing_id, spin_period, dm, pdot, pdotdot, fold_snr, filename, filepath, search_candidate_id, dp_id, metadata_hash=None):
    '''
    Insert a new fold_candidate into fold_candidate. No unique check as there will be millions of candidates.
    '''
    fold_candidate_table = get_table("fold_candidate")
    # combined_args = f"{pointing_id}{beam_id}{processing_id}{spin_period}{dm}{pdot}{pdotdot}{fold_snr}{filename}{filepath}{search_candidate_id}{dp_id}"
    # # Generate SHA256 hash
    # metadata_hash = hashlib.sha256(combined_args.encode()).hexdigest()
    with engine.connect() as conn:
        stmt = insert(fold_candidate_table).values(pointing_id=pointing_id, beam_id=beam_id, processing_id=processing_id, spin_period=spin_period, dm=dm, pdot=pdot, pdotdot=pdotdot, fold_snr=fold_snr, filename=filename, filepath=filepath, search_candidate_id=search_candidate_id, metadata_hash=metadata_hash, dp_id=dp_id)
        conn.execute(stmt)
        conn.commit()
        print(f"Added fold candidate to fold_candidate table")

def insert_user(username, fullname, email, password_hash, administrator=1):
    '''
    Insert a new user into the user table if it doesn't already exist
    '''
    user_table = get_table("user")
    with engine.connect() as conn:
        # Check if a user with the same username exists
        stmt = (
            select(user_table)
            .where(user_table.c.username == username)
            .limit(1)
        )
        result = conn.execute(stmt).first()
        if result is None:
            stmt = insert(user_table).values(username=username, fullname=fullname, email=email, password_hash=password_hash, administrator=administrator)
            conn.execute(stmt)
            conn.commit()
            print(f"Added {fullname} to user table")
        else:
            print(f"{fullname} already exists in user table. Skipping...")

def insert_user_labels(fold_candidate_id, user_id, rfi=None, noise=None, t1_cand=None, t2_cand=None, known_pulsar=None, nb_psr=None, is_harmonic=None, is_confirmed_pulsar=None, pulsar_name=None):
    '''
    Insert a new user label into the user_labels table.
    '''
    user_labels_table = get_table("user_labels")
    with engine.connect() as conn:
        #Add user labels to user_labels table        
        stmt = insert(user_labels_table).values(fold_candidate_id=fold_candidate_id, user_id=user_id, rfi=rfi, noise=noise, t1_cand=t1_cand, t2_cand=t2_cand, known_pulsar=known_pulsar, nb_psr=nb_psr, is_harmonic=is_harmonic, is_confirmed_pulsar=is_confirmed_pulsar, pulsar_name=pulsar_name)
        conn.execute(stmt)
        conn.commit()
        print(f"Added user label to user_labels table")

def insert_beam_antenna(antenna_id, beam_id, description=None):
    '''
    Insert a new beam_antenna into the beam_antenna table if it doesn't already exist
    '''
    beam_antenna_table = get_table("beam_antenna")
    with engine.connect() as conn:
        # Check if a beam_antenna with the same antenna_id and beam_id exists
        stmt = (
            select(beam_antenna_table)
            .where(beam_antenna_table.c.antenna_id == antenna_id)
            .where(beam_antenna_table.c.beam_id == beam_id)
            .limit(1)
        )
        result = conn.execute(stmt).first()
        if result is None:
            stmt = insert(beam_antenna_table).values(antenna_id=antenna_id, beam_id=beam_id, description=description)
            conn.execute(stmt)
            conn.commit()
            print(f"Added beam_antenna to beam_antenna table")
        else:
            print(f"Beam_antenna already exists in beam_antenna table. Skipping...")

 # A combination of unique utc_start, project_name, telescope_name, freq_band and target_name identifies a unique pointing.


def main():
    
    

 
    
    # insert_project_name("COMPACT", "ERC funded baseband pulsar search survey with MeerKAT and Effelsberg")
    # insert_project_name("TRAPUM_GC_SEARCHES", "GC Searches for the Transients and Pulsars with MeerKAT survey")
    # insert_project_name("HTRU_S_LOWLAT", "HTRU South Low latitude survey with Parkes")
    # print_table("project")

    # insert_file_type("filterbank", "Filterbank file")
    # insert_file_type("psrfits", "PSRFITS file")
    # insert_file_type("csv", "CSV file")
    # insert_file_type("png", "PNG file")
    # insert_file_type("pdf", "PDF file")
    # insert_file_type("txt", "Text file")
    # insert_file_type("json", "JSON file")
    # insert_file_type("xml", "XML file")
    # insert_file_type("yaml", "YAML file")
    # insert_file_type("dat", "PRESTO time series file")
    # insert_file_type("tim", "Sigproc time series file")
    # insert_file_type("ar", "Folded archive file")
    # insert_file_type("pfd", "PRESTO folded archive file")
    # insert_file_type("gz", "GZIP compressed file")
    # print_table("file_type")

    
   
   
    # insert_telescope_name("MeerKAT", "Radio Inteferometer in South Africa")
    # insert_telescope_name("Effelsberg", "Single-Dish Radio telescope in Germany")
    # insert_telescope_name("Parkes", "Single-Dish Radio telescope in Australia")
    # insert_telescope_name("GBT", "Robert C. Byrd Green Bank Telescope in West Virginia")
    # print_table("telescope")

    # for i in range(64):
    #     insert_antenna(f"MK{i:03d}", "MeerKAT", description= f"MeerKAT Antenna {i}")

    # insert_antenna("EF000", "Effelsberg", description= "Effelsberg Antenna")
    # insert_antenna("PK000", "Parkes", description= "Parkes Antenna")
    # insert_antenna("GB000", "GBT", description= "GBT Antenna")
    # print_table("antenna")
   
    # insert_target_name("J2140-2310B", "21:40:22.4100", "-23:10:48.8000", "TRAPUM_GC_SEARCHES", description= "APSUSE Observation with beam on M30B")
    # insert_target_name("J2140-2310A", "21:40:22.4100", "-23:10:48.8000", "COMPACT", 0.06, 0.2, 0.3, 0.4, 0.5, 0.2, "COMPACT Observation with beam on M30A")
    # insert_target_name("J2140-2310A", "21:40:22.4100", "-23:10:46.8000", "COMPACT", description= "COMPACT Observation with beam on M30A")
    # print_table("target")
    # #test = insert_pointing("2021-01-30-11:54:02.03986", 3573.05229549, 4096, "LBAND", "J2140-2310A", decimal.Decimal(900.0), decimal.Decimal(1500.0), decimal.Decimal(0.000256), "MeerKAT", "L-band", return_id=True)
    # #print(test)
    # print_table("pointing")
    # insert_pointing("2021-01-30-11:54:02.03986", 3573.05229549, 4096, "LBAND", "J2140-2310A", decimal.Decimal(900.0), decimal.Decimal(1500.0), decimal.Decimal(0.000256), "MeerKAT", "L-band")
    # print_table("pointing") 



    # insert_beam_type("Stokes_I", "Total Intensity Beam")
    # insert_beam_type("Baseband", "Baseband voltage beam")
    # insert_beam_type("test", "test voltage beam")
    # print_table("beam_type")
    
    # # #insert_beam("cfbf00000", "21:40:22.4100", "-23:10:48.8000", 1, 1, 0.000256, True)
    # test1 = insert_beam_without_pointing_id("cfbf00002", "21:40:12.4100", "-23:10:48.8000", "Stokes_I", "2021-01-30-11:54:02.05986", "COMPACT", "MeerKAT", "J2140-2310A", "LBAND", 0.000256, is_coherent=True, return_id=True)
    # print(test1)
    # print_table("beam")
    # #insert_beam_without_pointing_id("cfbf00002", "21:40:12.4100", "-23:10:48.8000", "Stokes_I", "2021-01-30-11:54:02.05986", "COMPACT", "MeerKAT", "J2140-2310A", "LBAND", 0.000256, is_coherent=True)
    # #test1 = insert_beam_without_pointing_id("cfbf00002", "21:40:12.4100", "-23:10:48.8000", "Stokes_I", "2021-01-30-11:54:02.05986", "COMPACT", "MeerKAT", "J2140-2310A", "LBAND", 0.000256, is_coherent=True, return_id=True)
    # #print_table("beam")
    # #print(test1)

    #insert_beam_antenna(1, 1, description="Incoherent beam 1")
    #print_table("beam_antenna")

    # insert_hardware("Contra", "ht-condor", "Contra HPC in Dresden")
    # insert_hardware("Hercules", "slurm", "Hercules HPC in Garching")
    # insert_hardware("OzSTAR", "slurm", "OzSTAR HPC in Melbourne")
    # insert_hardware("Ngarrgu", "slurm", "Ngarrgu Tindebeek HPC in Melbourne")
    # insert_hardware("AWS", "aws", "Amazon Web Services")
    # print_table("hardware")
    
    # insert_pipeline("Peasoup", "peasoup", "123456", "main", "Peasoup Time-Domain Acceleration Search Pipeline")
    # insert_pipeline("Presto", "presto", "123456", "main", "PRESTO Frequency-Domain Acceleration Search Pipeline")
    # print_table("pipeline")
    # insert_peasoup(-10.0, 10.0, 9.0, 50.0, 16, 1, 10000, 4294967296, "dm_list.txt", 1.11, "birdies.txt", "mask.txt", "--nsub 32 --npart 32", "docker.io/compact/peasoup", "1.0", "docker", "abcde", "123456", 1)
    # print_table("peasoup")
    # insert_data_product(1, "filterbank", "Ter5_cfbf00000.fil", "/datax/scratch/compact/cbf/cbf00000", "123456", True, "2021-01-30-11:54:02.05986", "2021-01-30-11:54:02.05986", "metainfo", False, "2021-01-30-11:54:02.05986", 0.000256, 900.0, 1500.0, 4096, 900.0, None, "Hercules", tstart=60000.0, fft_size=16777216)
    # insert_data_product(1, "filterbank", "Ter5_cfbf00001.fil", "/datax/scratch/compact/cbf/cbf00000", "123456", True, "2021-01-30-11:54:02.05986", "2021-01-30-11:54:02.05986", "metainfo", False, "2021-01-30-11:54:02.05986", 0.000256, 900.0, 1500.0, 4096, 900.0, None, "Hercules", tstart=60000.0, fft_size=16777216)
    # insert_data_product(1, "filterbank", "Ter5_cfbf00002.fil", "/datax/scratch/compact/cbf/cbf00000", "123456", True, "2021-01-30-11:54:02.05986", "2021-01-30-11:54:02.05986", "metainfo", False, "2021-01-30-11:54:02.05986", 0.000256, 900.0, 1500.0, 4096, 900.0, None, "Hercules", tstart=60000.0, fft_size=16777216)

    #print_table("data_product")

    # insert_pulsarx(64, 32, 2.0, 64, 100, "zdot", "--nsub 32 --npart 32", 2, "docker.io/compact/pulsarx", "1.0", "docker", "abcde", "123456", 2)
    # print_table("pulsarx")
    # insert_prepfold(32, "mask.txt", "--nsub 32 --npart 32", "docker.io/compact/prepfold", "1.0", "docker", "abcde", "123456", 3)
    # print_table("prepfold")
    # insert_filtool("kadane", "MeerKAT", 12, "--nsub 32 --npart 32", "docker.io/compact/filtool", "1.0", "docker", "abcde", "123456", 4)
    # print_table("filtool")
    #insert_circular_orbit_search(1, 100.0, 1.2, 10.0, 0.0, 2 * func.pi(), 0.9, 0.2, "docker.io/compact/circular_orbit_search", "1.0", "docker", "abcde", "123456", 5)
    #print_table("circular_orbit_search")
    #insert_elliptical_orbit_search(1, 100.0, 1.2, 10.0, 0.0, 2 * func.pi(), 0.0, 0.5, 0.0, 2 * func.pi(), 0.9, 0.1, "docker.io/compact/elliptical_orbit_search", "1.0", "docker", "abcde", "123456", 6)
    #print_table("elliptical_orbit_search")
    #insert_rfifind(2.0, 4.0, 2.0, 0.7, 0.3, 4, "--nsub 32 --npart 32", "docker.io/compact/rfifind", "1.0", "docker", "abcde", "123456", 7)
    #print_table("rfifind")
   
    
    # insert_processing([1], "123456", "Hercules", "2021-01-30-11:54:02.05986", "SUBMITTED", 1, 3, 1, "peasoup", "82ce5f6b29812d30595576dc7c43826c79da18e788ccf3559f06b541ffa247c4") 
    # insert_processing([1], "123456", "Hercules", "2021-01-30-11:54:02.05986", "SUCCESS", 1, 3, 2, "pulsarx", "7868d85ceb2a461af964bcb4fc45842b7c560fdd16080cd37f2cbf0f1de79bb6", start_time="2021-01-30-11:54:02.05986", end_time="2021-01-30-12:54:02.05986")

    # insert_processing([2,3], "123456", "Hercules", "2021-01-30-11:54:02.05986", "RUNNING", 1, 3, 1, "peasoup", "82ce5f6b29812d30595576dc7c43826c79da18e788ccf3559f06b541ffa247c4", start_time="2021-01-30-11:54:02.05986")
    # insert_processing([2,3], "123456", "Hercules", "2021-01-30-11:54:02.05986", "SUCCESS", 1, 3, 2, "pulsarx", "7868d85ceb2a461af964bcb4fc45842b7c560fdd16080cd37f2cbf0f1de79bb6", start_time="2021-01-30-11:54:02.05986", end_time="2021-01-30-13:54:02.05986") 
    # insert_processing([2], "123456", "Hercules", "2021-01-30-11:54:02.05986", "RUNNING", 1, 3, 1, "peasoup", "82ce5f6b29812d30595576dc7c43826c79da18e788ccf3559f06b541ffa247c4", start_time="2021-01-30-11:54:02.05986") 

    # print_table("processing")
    # print_table("processing_dp_inputs")
    # #Status Update
    # insert_processing([1], "123456", "Hercules", "2021-01-30-11:54:02.05986", "RUNNING", 1, 3, 1, "peasoup", "82ce5f6b29812d30595576dc7c43826c79da18e788ccf3559f06b541ffa247c4", start_time="2021-01-30-11:54:02.05986") 
    # print_table("processing")
    # # #Status Update
    # insert_processing([1], "123456", "Hercules", "2021-01-30-11:54:02.05986", "SUCCESS", 1, 3, 1, "peasoup", "82ce5f6b29812d30595576dc7c43826c79da18e788ccf3559f06b541ffa247c4", start_time="2021-01-30-11:54:02.05986", end_time="2021-01-30-12:54:02.05986")
    # insert_processing([2,3], "123456", "Hercules", "2021-01-30-11:54:02.05986", "SUCCESS", 1, 3, 1, "peasoup", "82ce5f6b29812d30595576dc7c43826c79da18e788ccf3559f06b541ffa247c4", start_time="2021-01-30-11:54:02.05986", end_time="2021-01-30-13:54:02.05986")
    # insert_candidate_filter_name("zero_dm", "zero dm filter")
    # print_table("candidate_filter")
    #insert_search_candidate(1, 1, 1, 0.02112, 200.0, 50.0, "Ter5_cfbf00000.xml", "/datax/scratch/compact/cbf/cbf00000", 4, 1, 1, pdot=1e-10, pdotdot=0.0)
    #print_table("search_candidate")

    # insert_fold_candidate(1, 1, 1, 0.05111, 200.0, 1e-9, 0.0, 50.0, "Ter5_cfbf00000.ar", "/datax/scratch/compact/cbf/cbf00000", 2, 1)
    # print_table("fold_candidate")
    #insert_user("vishnu", "Vishnu Balakrishnan", "vishnu@mpifr-bonn.mpg.de", "123456", administrator=1)
    #insert_user("vivek", "Vivek Krishnan", "vkrishnan@mpifr-bonn.mpg.de", "654321", administrator=1)
    # print_table("user")
    
    # insert_user_labels(1, 1, rfi=1)
    # insert_user_labels(1, 2, noise=1)
    # print_table("user_labels")

    
    



   

if __name__ == '__main__':
    main()
