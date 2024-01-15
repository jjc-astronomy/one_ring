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
engine = create_engine(connection_url)
metadata_obj = MetaData()
#telescope_table = Table("telescope", metadata_obj, autoload_with=engine)
target_table = Table("target", metadata_obj, autoload_with=engine)
pointing_table = Table("pointing", metadata_obj, autoload_with=engine)
stmt = select(pointing_table)

#Delete all rows from the table
#stmt = telescope_table.delete()

# with engine.connect() as conn:
#     result = conn.execute(stmt)
#     conn.commit()
# sys.exit()

with engine.connect() as conn:
    result = conn.execute(stmt)
    for row in result:
        print(row)

# sys.exit()

# #Orm method
# session = Session(engine)
# row = session.execute(select(telescope_table)).first()
# print(row)

def insert_telescope_name(engine, telescope_name, telescope_description):
    metadata_obj = MetaData()
    telescope_table = Table("telescope", metadata_obj, autoload_with=engine)
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

def insert_project_name(engine, project_name, project_description):
    metadata_obj = MetaData()
    project_table = Table("project", metadata_obj, autoload_with=engine)
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

def insert_target_name(engine, target_name, ra, dec, project_name, description=None):
    '''
    Insert a new target into the target table if it doesn't already exist for the same project
    '''
    metadata_obj = MetaData()
    target_table = Table("target", metadata_obj, autoload_with=engine)
    project_table = Table("project", metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        #join target and project tables
        stmt = select(target_table).join(project_table).where(target_table.c.source_name == target_name).limit(1)
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
            print(f"{target_name} already exists in target table. Skipping...")


def delete_all_rows(engine, table_name):
    metadata_obj = MetaData()
    table = Table(table_name, metadata_obj, autoload_with=engine)
    with engine.connect() as conn:
        stmt = table.delete()
        conn.execute(stmt)
        conn.commit()
        print(f"Deleted all rows from {table_name} table")


def reset_primary_key_counter(engine, table_name):
    with engine.connect() as conn:
        stmt = text(f"ALTER TABLE {table_name} AUTO_INCREMENT = 1")
        conn.execute(stmt)
        conn.commit()
        print(f"Reset primary key counter for {table_name} table")

def insert_pointing(engine, utc_start_str, tobs, nchans, freq_band, target_name, freq_start_mhz, freq_end_mhz, tsamp_seconds, telescope_name, project_name, receiver_name=None):
    '''
    Insert a new pointing into the pointing table if it doesn't already exist for the same target at the same UTC start time for the same project and telescope
    '''
    metadata_obj = MetaData()
    pointing_table = Table("pointing", metadata_obj, autoload_with=engine)
    target_table = Table("target", metadata_obj, autoload_with=engine)
    telescope_table = Table("telescope", metadata_obj, autoload_with=engine)
    project_table = Table("project", metadata_obj, autoload_with=engine)
    
    utc_start = datetime.strptime(utc_start_str, '%Y-%m-%d-%H:%M:%S.%f')
    utc_start = utc_start.replace(microsecond=0)

    with engine.connect() as conn:
        #join target and project tables
        
        stmt = (
            select(pointing_table)
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
            #get target id
            stmt = select(target_table).where(target_table.c.source_name == target_name).limit(1)
            result = conn.execute(stmt).first()
            target_id = result[0]
            #get project id
            stmt = select(project_table).where(project_table.c.name == project_name).limit(1)
            result = conn.execute(stmt).first()
            project_id = result[0]
            #get telescope id
            stmt = select(telescope_table).where(telescope_table.c.name == telescope_name).limit(1)
            result = conn.execute(stmt).first()
            telescope_id = result[0]
            stmt = insert(pointing_table).values(utc_start=utc_start, tobs=tobs, nchan_raw=nchans, freq_band=freq_band, target_id=target_id, freq_start_mhz=freq_start_mhz, freq_end_mhz=freq_end_mhz, tsamp_raw_seconds=tsamp_seconds, telescope_id=telescope_id, project_id=project_id, receiver=receiver_name)
            conn.execute(stmt)
            conn.commit()
            print(f"Added pointing for {target_name} for project {project_name}, observed at {utc_start} with telescope {telescope_name} to pointing table")
        else:
            print(f"Pointing for {target_name} at {utc_start} already exists in pointing table. Skipping...")

# insert_telescope_name(engine, "MeerKAT", "Radio Inteferometer in South Africa")
# insert_telescope_name(engine, "Effelsberg", "Single-Dish Radio telescope in Germany")
# insert_telescope_name(engine, "Parkes", "Single-Dish Radio telescope in Australia")
# insert_project_name(engine, "COMPACT", "ERC funded project")
#delete_all_rows(engine, "pointing")
#reset_primary_key_counter(engine, "pointing")
# insert_target_name(engine, "J2140-2310A", "21:40:22.4100", "-23:10:48.8000", "TRAPUM_GC_SEARCHES", "APSUSE Observation with beam on M30A")
insert_pointing(engine, "2021-01-30-11:44:01.05986", 3573.05229549, 4096, "L", "J2140-2310A", 900, 1500, 0.000256, "MeerKAT", "TRAPUM_GC_SEARCHES", "L-band")



# # printable_query = query_get_filterbank_files 
# # print(printable_query)

# # df = pd.read_sql_query(query_get_filterbank_files, con=cnx)
# # print(df)