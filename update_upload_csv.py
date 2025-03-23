import numpy as np
import pandas as pd
import sys, os
import logging


df = pd.read_csv('2HM_21012025.csv')
df['bf_utc'] = "2024.05.19 16:49:28.797"
df['hardware_name'] = "hercules"

df["filenames"] = df["filenames"].str.replace("/b/PROCESSING", "/mandap/incoming/meertrans/COMPACT", regex=False)
df["stream_id"] = df["filstr"].str.extract(r"^(\d+)")

df['stream_id'] = df['stream_id'].astype(np.int64)
df['bf_ra'] = "5:14:06.70"
df['bf_dec'] = "-40:02:48.9"
df['beam_type'] = "STOKES_I"
#Select rows with stream_id 4

df = df.loc[df['stream_id'] == 4]

# Function to check if all filenames exist and are non-empty
def files_exist(filenames_str):
    filenames = filenames_str.split()  # Split space-separated filenames
    missing_files = [f for f in filenames if not os.path.isfile(f) or os.path.getsize(f) == 0]
    
    if missing_files:
        for f in missing_files:
            logging.warning(f"File does not exist or is empty: {f}")
        return False
    return True

# Filter dataframe to keep only rows where all filenames exist and have nonzero size
df = df[df["filenames"].apply(files_exist)]
del df['stream_id']
df.to_csv('J0514-4002A_2HM_STREAM_ID_4.csv', index=False)
print(df)