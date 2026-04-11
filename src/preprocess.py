import pandas as pd
import geopandas as gpd
from shapely.geometry import box
from pathlib import Path
import time
import logging

# Set logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s|%(asctime)s|%(message)s")


# Define paths
BASE_DIR = Path(__file__).parents[1]

RAW_DATA_DIR = BASE_DIR/"data"/"raw_data"
STUDY_AREA_PATH = BASE_DIR/"data"/"study_area/entrance.parquet"


# Define study area
MIN_LONG = -94.7674
MAX_LONG = -94.7297
MIN_LAT = 29.3387
MAX_LAT = 29.3559

# Define error codes
ERROR_CODES = {
        "SOG": 1023,
        "LON": 181,
        "LAT":91,
        "COG":0,
        "Heading":511
        }


def load_raw_csv(load_path):
    dfs = (pd.read_csv(file, parse_dates=["BaseDateTime"]) for file in load_path.glob("*.csv"))
    df = pd.concat(dfs, ignore_index=True)
    return df



def save_parquet(df, save_path):
    df.to_parquet(save_path)


def filter_bbox(df, min_longitude, max_longitude, min_latitude, max_latitude):
    df = df[
        (df["LON"] >= min_longitude) &
        (df["LON"] <= max_longitude) &
        (df["LAT"] >= min_latitude) &
        (df["LAT"] <= max_latitude)
    ]
    return df

def clean_error_values(df, error_codes):

    mask = pd.Series(True, index=df.index)
    for column, error_value in error_codes.items():
        mask = mask & (df[column] != error_value)
    df = df[mask]
    return df

def clean_error_ranges(df):
    df = df[df["SOG"] <= 60]
    return df

def main():
    
    logging.info("Fetching raw AIS data")    

    df = load_raw_csv(RAW_DATA_DIR)

    logging.info("Data retrieved")

    logging.info("Filtering to study area")

    df = filter_bbox(df, MIN_LONG, MAX_LONG, MIN_LAT, MAX_LAT)

    logging.info("Cleaning errors")

    df = clean_error_values(df, ERROR_CODES)
    
    df = clean_error_ranges(df)

    logging.info("Saving study area in parquet")

    save_parquet(df, STUDY_AREA_PATH)
    
    logging.info("Save complete")





if __name__ == "__main__":
    main()


