import requests
from bs4 import BeautifulSoup
import logging
import os
import pandas as pd
from datetime import datetime
from typing import Dict, List

os.makedirs("ny_taxi_section_2/data", exist_ok=True)
os.makedirs("ny_taxi_section_2/logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ny_taxi_section_2/logs/extraction_and_loading.log', mode = 'w'),
        logging.StreamHandler()
    ],
    force=True
)

FILTER_CONFIGS = [
    {'run_id': 1, 'day_of_week': 0, 'description': 'Monday'},
    {'run_id': 2, 'day_of_week': 1, 'description': 'Tuesday'},
    {'run_id': 3, 'day_of_week': 2, 'description': 'Wednesday'},
    {'run_id': 4, 'day_of_week': 3, 'description': 'Thursday'},
    {'run_id': 5, 'day_of_week': 4, 'description': 'Friday'},
    {'run_id': 6, 'day_of_week': 5, 'description': 'Saturday'},
    {'run_id': 7, 'day_of_week': 6, 'description': 'Sunday'},
]

class Extraction:
    """
    class to extract & preprocess NYC taxi

    1. Scrapes the NYC TLC site to find .parquet datasets
    2. Read parquet files into pandas dataframe
    3. filter by day of week
    4. adds metadata columns (run_id, extraction_time)

    """

    def __init__(self, url: str):
        self.url = url

    def get_links(self) -> List[str]:
        """
         Scrape the TLC webpage for links to relevant Parquet files
        """
        try:
            resp = requests.get(self.url)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            logging.info(f"successfully parse html from {self.url} -  code {resp.status_code}")

            links = []
            for a_tag in soup.find_all("a", href = True):
                href = a_tag["href"]
                if "yellow" in href and "2025-01" in href and href.endswith(".parquet"):
                    links.append(href)
                    logging.info(f"Links match: {href} ")
            logging.info(f"Found {len(links)} matching parquet")

        except Exception as e:
            logging.error(f"Get links error: {e}")
            raise
        
        return links
    
    def read_parquet(self, links: List[str]) -> Dict[str, pd.DataFrame]:
        """
        read parquet files as dataframes
        """
        dataframes = {}
        for link in links:
            if "yellow" in link:
                taxi_type = "yellow"
            else:
                taxi_type = "green"
        
            try:
                logging.info(f"Saving {taxi_type} as dataframes...")
                df = pd.read_parquet(link)
                dataframes[taxi_type] = df
                logging.info(f"Saved {taxi_type} as dataframes!")

            except Exception as e:
                logging.error(f"Failed to save {taxi_type} as dataframes!")
                raise
        return dataframes #original dataframes

    def filter_by_day_of_week (self, dataframes: Dict[str, pd.DataFrame], day_of_week = int) -> Dict[str, pd.DataFrame]:
        """
        Filter dataframes by day of week (0=Monday, 6=Sunday).
        - Automatically converts pickup datetime columns to pandas datetime.
        - Drops invalid or missing pickup timestamps.
        """
        filtered_dataframes = {}
        try:
            for taxi_type, df in dataframes.items():
                pickup_col= "tpep_pickup_datetime" if taxi_type == "yellow" else "lpep_pickup_datetime"
                if pickup_col not in df.columns:
                    logging.warning(f"No {pickup_col} columns found in {taxi_type} dataframe!")
                    continue

                #change pickup cols to datetime    
                df_copy = df.copy()
                df_copy[pickup_col] = pd.to_datetime(df_copy[pickup_col], errors = "coerce")

                #drop empty pickup cols
                invalid_rows = df_copy[pickup_col].isnull().sum()
                if invalid_rows > 0:
                    logging.info(f"found invalid rows {invalid_rows} in {taxi_type}, removing...")
                    df_copy.dropna(subset = [pickup_col], inplace = True)
                    logging.info(f"{len(df)} rows in {taxi_type} dataframe")

                #filter by day_of_week
                filtered_df = df_copy[df_copy[pickup_col].dt.day_of_week == day_of_week].copy()
                if filtered_df.empty:
                    logging.warning(f"No records found for {taxi_type} taxi on day_of_week={day_of_week}")
                else:
                    filtered_dataframes[taxi_type] = filtered_df
                    logging.info(f"Filtered {taxi_type} taxi: {len(filtered_df):,} records for day_of_week={day_of_week}")
        
        except Exception as e:
            logging.error(f"Filtering by day of week failed! error: {e}")

        return filtered_dataframes #filtered dataframes

    def metadata_addition(self, dataframes: Dict[str, pd.DataFrame], run_id: int) -> Dict[str, pd.DataFrame]:
        """
        metadata addition to filtered dataframes, 
        dataframes params as filtered dataframes
        """
        try:
            for taxi_type, df in dataframes.items():
                df["run_id"] = run_id
                df["extraction_time"] = pd.Timestamp.now()
                logging.info(f"Added metadata for {taxi_type}, run_id = {run_id}")

        except Exception as e:
            logging.error(f"Error adding metadata for {taxi_type}, run_id = {run_id}")
        
        return dataframes #modified

    def extract_all_data(self) -> Dict[int, Dict[str, pd.DataFrame]]:
        """
        Extract and process all taxi data
        """
        links = self.get_links()
        original_dataframe = self.read_parquet(links)

        runs_by_id = {} #{1: {'green': df, 'yellow': df}, 2:{'green': df, 'yellow': df}}

        try:
            for config in FILTER_CONFIGS:
                run_id = config["run_id"]
                day_of_week = config["day_of_week"]
                description = config["description"]

                filtered_dataframes = self.filter_by_day_of_week(original_dataframe, day_of_week)
                if not filtered_dataframes:
                    logging.warning(f"No data for run_id={run_id} ({description})")
                    continue

                modified_dataframes = self.metadata_addition(filtered_dataframes, run_id)
                
                if run_id not in runs_by_id:
                    runs_by_id[run_id] = {}
                
                for taxi_type, df in modified_dataframes.items():
                    df.columns = df.columns.str.lower()
                    runs_by_id[run_id][taxi_type] = df
                    logging.info(f"Completed run_id={run_id} with {len(modified_dataframes)} taxi type(s)")

        except Exception as e:
            logging.info(f"Error during extraction: {e}")
            raise

        return runs_by_id #used for loading per run_id

    
            


    