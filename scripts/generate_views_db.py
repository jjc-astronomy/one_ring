
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.engine import URL
import os
from dotenv import load_dotenv

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
observation_metadata = text("""
            CREATE VIEW observation_metadata AS
            SELECT 
            beam.id AS beam_id,
            beam.name AS beam_name,
            beam_types.id AS beam_type_id,
            beam_types.name AS beam_type,
            pointing.id AS pointing_id,
            pointing.utc_start AS utc_start,
            pointing.receiver AS receiver,
            pointing.freq_band AS freq_band,
            pointing.freq_start_mhz AS freq_start_mhz,
            pointing.freq_end_mhz AS freq_end_mhz,
            pointing.tsamp_raw_seconds AS tsamp_raw_seconds,
            target.id AS target_id,
            target.source_name AS target_name,
            project.id AS project_id,
            project.name AS project_name,
            telescope.id AS telescope_id,
            telescope.name AS telescope_name
            FROM beam
            JOIN beam_types ON beam_types.id = beam.beam_type_id
            JOIN pointing ON pointing.id = beam.pointing_id
            JOIN target ON target.id = pointing.target_id
            JOIN project ON project.id = pointing.project_id
            JOIN telescope ON telescope.id = pointing.telescope_id;
""")
metadata_obj.reflect(bind=engine)

# drop_view = text("DROP VIEW observation_metadata;")
# with engine.connect() as conn:
#     conn.execute(drop_view)


with engine.connect() as conn:
    conn.execute(observation_metadata)