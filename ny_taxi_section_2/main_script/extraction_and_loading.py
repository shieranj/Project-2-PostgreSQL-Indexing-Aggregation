from ny_taxi_section_2.helper.extraction import Extraction
from ny_taxi_section_2.helper.loading_to_postgres import PostgresConnector
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ny_taxi_section_2/logs/extraction_and_loading.log', mode = 'w'),
        logging.StreamHandler()
    ],
    force=True
)

DB_CONFIG = {
    'dbname': 'ny_taxi_capstone2_db',
    'user': 'project_capstone',
    'password': 'purwadika_capstone_two',
    'host': 'localhost',
    'port': 5432}

def main(): #load per run
    logging.info("="*80)
    logging.info("Extraction Starting...")
    logging.info("="*80)
    extraction = Extraction("https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page")
    runs_by_id = extraction.extract_all_data()
    
    if not runs_by_id:
        logging.error(f"Extraction Failed!")
    
    else:
        logging.info("Extraction Success!")

    logging.info("="*80)
    logging.info("Loading to Postgres Starting...")
    logging.info("="*80)

    logging.info("Loading per run_id...")
    pg = PostgresConnector(**DB_CONFIG)
    for run_id, taxi_dict in runs_by_id.items():
        for taxi_type, df in taxi_dict.items():
            if df.empty:
                logging.warning(f"Empty dataframe for {taxi_type}_{run_id}")
                continue

            table_name = f"nyc_{taxi_type}_2025-01"
            pg.load(table_name, df) 
            logging.info(f"{taxi_type}: Run {run_id} Loaded!")
    pg.close()
    logging.info("="*80)
    logging.info("Loading to Postgres Completed...")
    logging.info("="*80)

if __name__ == "__main__":
    main()






    

