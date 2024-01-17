import numpy as np
import pandas as pd
import sys
import argparse
import os
from dotenv import load_dotenv
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy import MetaData, Table, insert, select, text
from datetime import datetime
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
telescope_table = get_table("telescope")
project_table = get_table("project") 
target_table = get_table("target")   
pointing_table = get_table("pointing")
beam_table = get_table("beam")
beam_types_table = get_table("beam_types")
antenna_table = get_table("antenna")
hardware_table = get_table("hardware")
peasoup_table = get_table("peasoup")
pulsarx_table = get_table("pulsarx")
filtool_table = get_table("filtool")
prepfold_table = get_table("prepfold")
processing_table = get_table("processing")
#observation_metadata = get_table("observation_metadata")

stmt = select(target_table)
stmt1 = select(telescope_table)
stmt2 = select(project_table)
stmt3 = select(pointing_table)
stmt4 = select(antenna_table)
stmt5 = select(processing_table)
#stmt1 = select(target_table)

#Delete all rows from the table
#stmt = telescope_table.delete()

with engine.connect() as conn:
    result = conn.execute(stmt5)
    conn.commit()
    for row in result:
        print(row)

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

def insert_target_name(target_name, ra, dec, project_name, description=None):
    '''
    Insert a new target into the target table if it doesn't already exist for the same project.
    A different ra and dec will trigger a new entry in the target table
    '''
   
    target_table = get_table("target")
    project_table = get_table("project")

    with engine.connect() as conn:
        #join target and project tables
        stmt = (select(target_table).join(project_table)
                .where(target_table.c.source_name == target_name)
                .where(target_table.c.ra == ra)
                .where(target_table.c.dec == dec)
                .where(project_table.c.name == project_name)
                .limit(1))
        result = conn.execute(stmt).first()
     
        if result is None:
            #get project id
            stmt = select(project_table).where(project_table.c.name == project_name).limit(1)
            result = conn.execute(stmt).first()
            project_id = result[0]
            stmt = insert(target_table).values(source_name=target_name, ra=ra, dec=dec, notes=description, project_id=project_id)
            conn.execute(stmt)
            conn.commit()
            print(f"Added {target_name} to target table")
        else:
            print(f"{target_name} already exists with the given coordinates in target table for project {project_name}. Skipping...")


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

def insert_pointing(utc_start_str, tobs, nchans, freq_band, target_name, freq_start_mhz, freq_end_mhz, tsamp_seconds, telescope_name, project_name, receiver_name=None):
    '''
    Insert a new pointing into the pointing table if it doesn't already exist for the same target at the same UTC start time for the same project, telescope and freq band.
    '''
    
    pointing_table = get_table("pointing")
    target_table = get_table("target")
    telescope_table = get_table("telescope")
    project_table = get_table("project")
    
    utc_start = datetime.strptime(utc_start_str, '%Y-%m-%d-%H:%M:%S.%f')
    utc_start = utc_start.replace(microsecond=0)

    with engine.connect() as conn:

        stmt = (
            select(pointing_table)
            .join(target_table, target_table.c.id == pointing_table.c.target_id)
            .join(project_table, project_table.c.id == pointing_table.c.project_id)
            .join(telescope_table, telescope_table.c.id == pointing_table.c.telescope_id)
            .where(target_table.c.source_name == target_name)
            .where(project_table.c.name == project_name)
            .where(telescope_table.c.name == telescope_name)
            .where(pointing_table.c.utc_start == utc_start)
            .where(pointing_table.c.freq_band == freq_band)
            .limit(1)
        )

        result = conn.execute(stmt).first()
        
        if result is None:

            try:
                #get target id. 
                target_id = get_id_from_name("target", target_name, alternate_key='source_name')
            except:
                print(f"Target {target_name} does not exist in target table. Please add target first.")
                sys.exit()
            
            try:
                #get project id
                project_id = get_id_from_name("project", project_name)
             
            except:
                print(f"Project {project_name} does not exist in project table. Please add project first.")
                sys.exit()
            
            try:
                #get telescope id
                telescope_id = get_id_from_name("telescope", telescope_name)
            except:
                print(f"Telescope {telescope_name} does not exist in telescope table. Please add telescope first.")
                sys.exit()
            
            stmt = insert(pointing_table).values(utc_start=utc_start, tobs=tobs, nchan_raw=nchans, freq_band=freq_band, target_id=target_id, freq_start_mhz=freq_start_mhz, freq_end_mhz=freq_end_mhz, tsamp_raw_seconds=tsamp_seconds, telescope_id=telescope_id, project_id=project_id, receiver=receiver_name)
            conn.execute(stmt)
            conn.commit()
            print(f"Added pointing for {target_name} for project {project_name}, observed at {utc_start} with telescope {telescope_name} to pointing table")

        else:
            print(f"Pointing for {target_name} at {utc_start} already exists in pointing table. Skipping...")


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
        


def insert_beam_types(beam_type_name, description=None):
    '''
    Insert a new beam type into the beam_types table if it doesn't already exist
    '''
    beam_type_table = get_table("beam_types")

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

def get_pointing_id(target_name, utc_start_str, project_name, telescope_name):
    '''
    Get the pointing id for a given target, project, telescope and utc_start
    '''
    
    pointing_table = Table("pointing", metadata_obj, autoload_with=engine)
    target_table = Table("target", metadata_obj, autoload_with=engine)
    telescope_table = Table("telescope", metadata_obj, autoload_with=engine)
    project_table = Table("project", metadata_obj, autoload_with=engine)
    
    utc_start = datetime.strptime(utc_start_str, '%Y-%m-%d-%H:%M:%S.%f')
    utc_start = utc_start.replace(microsecond=0)

    with engine.connect() as conn:
        #join target and project tables
        stmt = (
            select(pointing_table.c.id)
            .join(target_table, target_table.c.id == pointing_table.c.target_id)
            .join(project_table, project_table.c.id == pointing_table.c.project_id)
            .join(telescope_table, telescope_table.c.id == pointing_table.c.telescope_id)
            .where(target_table.c.source_name == target_name)
            .where(project_table.c.name == project_name)
            .where(telescope_table.c.name == telescope_name)
            .where(pointing_table.c.utc_start == utc_start)
            .limit(1)
        )
        result = conn.execute(stmt).first()
        if result is None:
            return None
        else:
            return result[0]
    
def insert_beam(beam_name, beam_ra, beam_dec, beam_ra_str, beam_dec_str, pointing_id, beam_type_id):
    '''
    Insert a new beam into the beams table if it doesn't already exist
    '''

    beam_table = get_table("beam")
    beam_types_table = get_table("beam_types")
    pointing_table = get_table("pointing")

    with engine.connect() as conn:
        stmt = select(beam_table).where(beam_table.c.name == beam_name).where(beam_table.c.pointing_id == pointing_id).where(beam_table.c.beam_type_id == beam_type_id).limit(1)
        result = conn.execute(stmt).first()
        
        if result is None:
            stmt = insert(beam_table).values(name=beam_name, ra=beam_ra, dec=beam_dec, ra_str=beam_ra_str, dec_str=beam_dec_str, pointing_id=pointing_id, beam_type_id=beam_type_id)
            conn.execute(stmt)
            conn.commit()
            print(f"Added {beam_name} to beam table")
        else:
            print(f"{beam_name} already exists in beam table. Skipping...")

def insert_beam_without_pointing_id(beam_name, beam_ra, beam_dec, beam_ra_str, beam_dec_str, beam_type_name, utc_start_str, project_name, telescope_name, target_name, freq_band):
    '''
    Insert a new beam into the beams table if it doesn't already exist
    '''
    
    beam_table = get_table("beam")
    beam_types_table = get_table("beam_types")
    pointing_table = get_table("pointing")
    target_table = get_table("target")
    project_table = get_table("project")
    telescope_table = get_table("telescope")


    utc_start = datetime.strptime(utc_start_str, '%Y-%m-%d-%H:%M:%S.%f')
    utc_start = utc_start.replace(microsecond=0)

    with engine.connect() as conn:

        stmt = (
            select(beam_table)
            .join(beam_types_table, beam_types_table.c.id == beam_table.c.beam_type_id)
            .join(pointing_table, pointing_table.c.id == beam_table.c.pointing_id)
            .join(target_table, target_table.c.id == pointing_table.c.target_id)
            .join(project_table, project_table.c.id == pointing_table.c.project_id)
            .join(telescope_table, telescope_table.c.id == pointing_table.c.telescope_id)
            .where(beam_table.c.name == beam_name)
            .where(pointing_table.c.utc_start == utc_start)
            .where(target_table.c.source_name == target_name)
            .where(project_table.c.name == project_name)
            .where(telescope_table.c.name == telescope_name)
            .where(beam_types_table.c.name == beam_type_name)
            .where(pointing_table.c.freq_band == freq_band)
            .limit(1)
        )
        
        result = conn.execute(stmt).first()
        
       
        if result is None:
            #get beam type id
            beam_type_id = get_id_from_name("beam_types", beam_type_name)
            #get pointing id
            stmt = (
                   select(pointing_table)
                   .join(target_table, target_table.c.id == pointing_table.c.target_id)
                   .join(project_table, project_table.c.id == pointing_table.c.project_id)
                   .join(telescope_table, telescope_table.c.id == pointing_table.c.telescope_id)
                   .where(pointing_table.c.utc_start == utc_start)
                   .where(target_table.c.source_name == target_name)
                   .where(project_table.c.name == project_name)
                   .where(telescope_table.c.name == telescope_name)
                   .where(pointing_table.c.freq_band == freq_band)
                   .limit(1)
            )
            # Error checking
            try:
                result = conn.execute(stmt).first()
                pointing_id = result[0]
            except:
                print(f"The join statement to insert beam returned null. Likely one of the following keys {utc_start}, {target_name}, {project_name}, {telescope_name}, {freq_band}, does not exist in their respective tables. Please insert to that table and pointing first.")
                sys.exit()
            
            stmt = insert(beam_table).values(name=beam_name, ra=beam_ra, dec=beam_dec, ra_str=beam_ra_str, dec_str=beam_dec_str, pointing_id=pointing_id, beam_type_id=beam_type_id)
            conn.execute(stmt)
            conn.commit()
            print(f"Added {beam_name} to beam table")
        else:
            print(f"{beam_name} already exists in beam table. Skipping...")

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

def insert_antenna(antenna_name, telescope_name, antenna_description=None):
    '''
    Insert a new antenna into the antenna table if it doesn't already exist for the same telescope
    '''
    
    antenna_table = get_table("antenna")
    telescope_table = get_table("telescope")
    with engine.connect() as conn:
        
        stmt = (
            select(antenna_table)
            .join(telescope_table, telescope_table.c.id == antenna_table.c.telescope_id)
            .where(antenna_table.c.name == antenna_name)
            .where(telescope_table.c.name == telescope_name)
            .limit(1)
        )
        result = conn.execute(stmt).first()
        
        if result is None:
            #get telescope id
            telescope_id = get_id_from_name("telescope", telescope_name)
            stmt = insert(antenna_table).values(name=antenna_name, description=antenna_description, telescope_id=telescope_id)
            conn.execute(stmt)
            conn.commit()
            print(f"Added {antenna_name} to antenna table")
        else:
            print(f"{antenna_name} already exists in antenna table. Skipping...")

def insert_hardware(hardware_name, hardware_description=None):
    '''
    Insert a new hardware into the hardware table if it doesn't already exist
    '''
    
    hardware_table = get_table("hardware")
    with engine.connect() as conn:
        
        stmt = select(hardware_table).where(hardware_table.c.name == hardware_name).limit(1)
        result = conn.execute(stmt).first()
        
        if result is None:
            stmt = insert(hardware_table).values(name=hardware_name, description=hardware_description)
            conn.execute(stmt)
            conn.commit()
            print(f"Added {hardware_name} to hardware table")
        else:
            print(f"{hardware_name} already exists in hardware table. Skipping...")

def insert_pipeline(name, description=None):
    '''
    Insert a new pipeline into the pipeline table if it doesn't already exist
    '''
    
    pipeline_table = get_table("pipeline")
    with engine.connect() as conn:
        
        stmt = select(pipeline_table).where(pipeline_table.c.name == name).limit(1)
        result = conn.execute(stmt).first()
        
        if result is None:
            stmt = insert(pipeline_table).values(name=name, description=description)
            conn.execute(stmt)
            conn.commit()
            print(f"Added {name} to pipeline table")
        else:
            print(f"{name} already exists in pipeline table. Skipping...")

def insert_peasoup(acc_start, acc_end, min_snr, ram_limit_gb, nharmonics, ngpus, total_cands_limit, fft_size, dm_file, accel_tol, birdie_list, chan_mask, extra_args, container_image, container_version, container_type):
    ''' Insert Peasoup parameters into the peasoup_params table if it doesn't already exist ''' 
    peasoup_table = get_table("peasoup")
    with engine.connect() as conn:
            
            stmt = (
            select(peasoup_table)
            .where(peasoup_table.c.acc_start == acc_start)
            .where(peasoup_table.c.acc_end == acc_end)
            .where(peasoup_table.c.min_snr == min_snr)
            .where(peasoup_table.c.ram_limit_gb == ram_limit_gb)
            .where(peasoup_table.c.nharmonics == nharmonics)
            .where(peasoup_table.c.ngpus == ngpus)
            .where(peasoup_table.c.total_cands_limit == total_cands_limit)
            .where(peasoup_table.c.fft_size == fft_size)
            .where(peasoup_table.c.dm_file == dm_file)
            .where(peasoup_table.c.accel_tol == accel_tol)
            .where(peasoup_table.c.birdie_list == birdie_list)
            .where(peasoup_table.c.chan_mask == chan_mask)
            .where(peasoup_table.c.extra_args == extra_args)
            .where(peasoup_table.c.container_image == container_image)
            .where(peasoup_table.c.container_version == container_version)
            .where(peasoup_table.c.container_type == container_type)
            .limit(1)
            )
            result = conn.execute(stmt).first()
            
            if result is None:
                stmt = insert(peasoup_table).values(acc_start=acc_start, acc_end=acc_end, min_snr=min_snr, ram_limit_gb=ram_limit_gb, nharmonics=nharmonics, ngpus=ngpus, total_cands_limit=total_cands_limit, fft_size=fft_size, dm_file=dm_file, accel_tol=accel_tol, birdie_list=birdie_list, chan_mask=chan_mask, extra_args=extra_args, container_image=container_image, container_version=container_version, container_type=container_type)
                conn.execute(stmt)
                conn.commit()
                print(f"Added Peasoup parameters to peasoup_params table")
            else:
                print(f"Peasoup parameters already exist in peasoup_params table. Skipping...")

def insert_pulsarx(subbands_number, subint_length, clfd_q_value, fast_period_bins, slow_period_bins, rfi_filter, extra_args, threads, container_image, container_version, container_type):
    '''
    Insert PulsarX parameters into the pulsarx_params table if it doesn't already exist
    '''
    pulsarx_table = get_table("pulsarx")
    with engine.connect() as conn:
                
                stmt = (
                select(pulsarx_table)
                .where(pulsarx_table.c.subbands_number == subbands_number)
                .where(pulsarx_table.c.subint_length == subint_length)
                .where(pulsarx_table.c.clfd_q_value == clfd_q_value)
                .where(pulsarx_table.c.fast_nbins == fast_period_bins)
                .where(pulsarx_table.c.slow_nbins == slow_period_bins)
                .where(pulsarx_table.c.rfi_filter == rfi_filter)
                .where(pulsarx_table.c.extra_args == extra_args)
                .where(pulsarx_table.c.threads == threads)
                .where(pulsarx_table.c.container_image == container_image)
                .where(pulsarx_table.c.container_version == container_version)
                .where(pulsarx_table.c.container_type == container_type)
                .limit(1)
                )
                result = conn.execute(stmt).first()
                
                if result is None:
                    stmt = insert(pulsarx_table).values(subbands_number=subbands_number, subint_length=subint_length, clfd_q_value=clfd_q_value, fast_nbins=fast_period_bins, slow_nbins=slow_period_bins, rfi_filter=rfi_filter, extra_args=extra_args, threads=threads, container_image=container_image, container_version=container_version, container_type=container_type)
                    conn.execute(stmt)
                    conn.commit()
                    print(f"Added PulsarX parameters to pulsarx_params table")
                else:
                    print(f"PulsarX parameters already exist in pulsarx_params table. Skipping...")

def insert_filtool(rfi_filter, telescope_name, threads, extra_args, container_image, container_version, container_type):
    '''
    Insert Filtool parameters into the filtool_params table if it doesn't already exist
    '''
    filtool_table = get_table("filtool")
    with engine.connect() as conn:
                    
                    stmt = (
                    select(filtool_table)
                    .where(filtool_table.c.rfi_filter == rfi_filter)
                    .where(filtool_table.c.telescope_name == telescope_name)
                    .where(filtool_table.c.threads == threads)
                    .where(filtool_table.c.extra_args == extra_args)
                    .where(filtool_table.c.container_image == container_image)
                    .where(filtool_table.c.container_version == container_version)
                    .where(filtool_table.c.container_type == container_type)
                    .limit(1)
                    )
                    result = conn.execute(stmt).first()
                    
                    if result is None:
                        stmt = insert(filtool_table).values(rfi_filter=rfi_filter, telescope_name=telescope_name, threads=threads, extra_args=extra_args, container_image=container_image, container_version=container_version, container_type=container_type)
                        conn.execute(stmt)
                        conn.commit()
                        print(f"Added Filtool parameters to filtool_params table")
                    else:
                        print(f"Filtool parameters already exist in filtool_params table. Skipping...")

def insert_prepfold(ncpus, rfifind_mask, extra_args, container_image, container_version, container_type):
    '''
    Insert Prepfold parameters into the prepfold_params table if it doesn't already exist
    '''
    prepfold_table = get_table("prepfold")
    with engine.connect() as conn:
                            
                            stmt = (
                            select(prepfold_table)
                            .where(prepfold_table.c.ncpus == ncpus)
                            .where(prepfold_table.c.rfifind_mask == rfifind_mask)
                            .where(prepfold_table.c.extra_args == extra_args)
                            .where(prepfold_table.c.container_image == container_image)
                            .where(prepfold_table.c.container_version == container_version)
                            .where(prepfold_table.c.container_type == container_type)
                            .limit(1)
                            )
                            result = conn.execute(stmt).first()
                            
                            if result is None:
                                stmt = insert(prepfold_table).values(ncpus=ncpus, rfifind_mask=rfifind_mask, extra_args=extra_args, container_image=container_image, container_version=container_version, container_type=container_type)
                                conn.execute(stmt)
                                conn.commit()
                                print(f"Added Prepfold parameters to prepfold_params table")
                            else:
                                print(f"Prepfold parameters already exist in prepfold_params table. Skipping...")

def insert_processing(pipeline_name, hardware_name, submit_time, attempt_number, max_attempts, process_status, start_time=None, end_time=None):
    '''
    Insert a new processing into the processing table if it doesn't already exist
    '''
    processing_table = get_table("processing")
    pipeline_table = get_table("pipeline")
    hardware_table = get_table("hardware")
    with engine.connect() as conn:
                                    
        stmt = (
        select(processing_table)
        .join(pipeline_table, pipeline_table.c.id == processing_table.c.pipeline_id)
        .join(hardware_table, hardware_table.c.id == processing_table.c.hardware_id)
        .where(pipeline_table.c.name == pipeline_name)
        .where(hardware_table.c.name == hardware_name)
        .where(processing_table.c.submit_time == submit_time)
        .where(processing_table.c.start_time == start_time)
        .where(processing_table.c.end_time == end_time)
        .where(processing_table.c.process_status == process_status)
        .where(processing_table.c.attempt_number == attempt_number)
        .where(processing_table.c.max_attempts == max_attempts)
        .limit(1)
        )
        result = conn.execute(stmt).first()
        
        if result is None:
            #get pipeline id
            pipeline_id = get_id_from_name("pipeline", pipeline_name)
            #get hardware id
            hardware_id = get_id_from_name("hardware", hardware_name)
            stmt = insert(processing_table).values(pipeline_id=pipeline_id, hardware_id=hardware_id, submit_time=submit_time, start_time=start_time, end_time=end_time, process_status=process_status, attempt_number=attempt_number, max_attempts=max_attempts)
            conn.execute(stmt)
            conn.commit()
            print(f"Added processing to processing table")
        else:
            print(f"Processing already exists in processing table. Skipping...")
                                
#Creating a view of all the metadata for an observation

def main():
    #insert_target_name("J2140-2310B", "21:40:22.4100", "-23:10:48.8000", "TRAPUM_GC_SEARCHES", "APSUSE Observation with beam on M30B")
    # insert_target_name("J2140-2310A", "21:40:22.4100", "-23:10:48.8000", "COMPACT", "COMPACT Observation with beam on M30A")
    # insert_target_name("J2140-2310A", "21:40:22.4100", "-23:10:46.8000", "COMPACT", "COMPACT Observation with beam on M30A")
    #insert_pointing("2021-01-30-11:54:02.03986", 3573.05229549, 4096, "LBAND", "J2140-2310A", 900, 1500, 0.000256, "MeerKAT", "COMPACT", "L-band")
    # insert_telescope_name("GBT", "Robert C. Byrd Green Bank Telescope in West Virginia")
    # insert_project_name("random", "Random unfunded project")
    # insert_beam_types("Stokes_I", "Total Intensity Beam")
    # insert_beam_types("Baseband", "Baseband voltage beam")
    # insert_beam_without_pointing_id("cfbf00005", 325.093375, -23.18022222222222, "21:40:22.4100", "-23:10:48.8000", "Stokes_I", "2021-01-30-11:54:02.05986", "COMPACT", "MeerKAT", "J2140-2310A", "LBAND")
    # delete_all_rows("antenna")
    # reset_primary_key_counter("antenna")
    # for i in range(64):
    #     insert_antenna(f"MK{i:02d}", "MeerKAT", f"MeerKAT Antenna {i + 1}")
    #insert_antenna("EF00", "Effelsberg", "Effelsberg Antenna")
    # insert_antenna("PK00", "Parkes", "Parkes Antenna")
    # insert_antenna("GB00", "GBT", "GBT Antenna")
    #delete_all_rows("hardware")
    #reset_primary_key_counter("hardware")
    # insert_hardware("Contra", "Contra HPC in Dresden")
    # insert_hardware("Hercules", "Hercules HPC in Garching")
    # insert_hardware("OzSTAR", "OzSTAR HPC in Melbourne")
    # insert_hardware("Ngarrgu", "Ngarrgu Tindebeek HPC in Melbourne")
    # insert_hardware("AWS", "Amazon Web Services")
    # insert_pipeline("Peasoup", "Peasoup Time-Domain Acceleration Search Pipeline")
    # insert_pipeline("PRESTO", "PRESTO Frequency-Domain Acceleration Search Pipeline")
    #insert_pipeline("Peasoup Accel Search Full Length", "Peasoup Time-Domain Acceleration Search Pipeline")
    #sys.exit()
    #insert_peasoup(-10.0, 10.0, 9.0, 50.0, 16, 1, 10000, 4294967296, "dm_list.txt", 1.0, "birdies.txt", "mask.txt", "--nsub 32 --npart 32", "docker.io/compact/peasoup", 1.0, 2)
    #subbands_number, subint_length, clfd_q_value, fast_period_bins, slow_period_bins, rfi_filter, extra_args, threads, container_image, container_version, container_type
    insert_pulsarx(64, 32, 0.5, 64, 100, "zdot", "--nsub 32 --npart 32", 2, "docker.io/compact/pulsarx", 1.0, 2)
    #rfi_filter, telescope_name, threads, extra_args, container_image, container_version, container_type
    #ncpus, rfifind_mask, extra_args, container_image, container_version, container_type
    #insert_filtool("kadane", "MeerKAT", 2, "--nsub 32 --npart 32", "docker.io/compact/filtool", 1.0, 2)
    #insert_prepfold(12, "mask.txt", "--nsub 32 --npart 32", "docker.io/compact/prepfold", 1.0, 2)
    insert_processing("Peasoup Accel Search Full Length", "Contra", "2021-01-30-11:54:02.05986", 1, 3, "SUCCESS", "2021-01-30-12:54:02.05986", "2021-01-30-12:59:02.05986")
    insert_processing("Peasoup Accel Search Full Length", "Contra", "2021-01-30-11:54:02.05986", 1, 3, "SUBMITTED")




if __name__ == '__main__':
    main()
    #insert_file_type(engine, "pfd", "PRESTO folded archive file")
# insert_telescope_name(engine, "MeerKAT", "Radio Inteferometer in South Africa")
# insert_telescope_name(engine, "Effelsberg", "Single-Dish Radio telescope in Germany")
# insert_telescope_name(engine, "Parkes", "Single-Dish Radio telescope in Australia")
# insert_project_name(engine, "COMPACT", "ERC funded project")
#delete_all_rows(engine, "pointing")
#reset_primary_key_counter(engine, "pointing")
#insert_pointing(engine, "2021-01-30-11:44:01.05986", 3573.05229549, 4096, "L", "J2140-2310A", 900, 1500, 0.000256, "MeerKAT", "TRAPUM_GC_SEARCHES", "L-band")
#insert_beam_types(engine, "Stokes_I", "Total Intensity Beam")
#id = get_pointing_id(engine, "J2140-2310A", "2021-01-30-11:44:01.05986", "TRAPUM_GC_SEARCHES", "MeerKAT")
#insert_beam(engine, "cfbf00000", 325.093375, -23.18022222222222, "21:40:22.4100", "-23:10:48.8000", "Stokes_I")
#insert_beam_without_pointing_id(engine, "cfbf00002", 325.093375, -23.18022222222222, "21:40:22.4100", "-23:10:48.8000", "Stokes_I", "2021-01-30-11:44:01.05986", "TRAPUM_GC_SEARCHES", "MeerKAT", "J2140-2310A")
#insert_beam(engine, "cfbf00002", 325.093375, -23.18022222222222, "21:40:22.4100", "-23:10:48.8000", 1, 1)
# insert_file_type(engine, "filterbank", "Filterbank file")
# insert_file_type(engine, "psrfits", "PSRFITS file")
# insert_file_type(engine, "csv", "CSV file")
# insert_file_type(engine, "png", "PNG file")
# insert_file_type(engine, "pdf", "PDF file")
# insert_file_type(engine, "txt", "Text file")
# insert_file_type(engine, "json", "JSON file")
# insert_file_type(engine, "xml", "XML file")
# insert_file_type(engine, "yaml", "YAML file")
# insert_file_type(engine, "dat", "PRESTO time series file")
# insert_file_type(engine, "tim", "Sigproc time series file")
# insert_file_type(engine, "ar", "Folded archive file")
# insert_file_type(engine, "pfd", "PRESTO folded archive file")

# # printable_query = query_get_filterbank_files 
# # print(printable_query)

# # df = pd.read_sql_query(query_get_filterbank_files, con=cnx)
# # print(df)

#What makes an observation unique.
#Target, utc_start, telescope, project, observing band
# Different frequencies (S1..S4), can be simulatenous! Add this to target, utc_start, telescope, project
# Different frequencies for different bands