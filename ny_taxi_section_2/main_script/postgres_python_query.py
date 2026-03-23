import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ny_taxi_section_2/logs/agreggation.log', mode = 'w'),
        logging.StreamHandler()
    ],
    force=True
)

OUTPUT_DIR="ny_taxi_section_2/data"
try:
    engine = create_engine("postgresql+psycopg2://project_capstone:purwadika_capstone_two@localhost:5432/ny_taxi_capstone2_db")
    logging.info("Connected to postgres")

except Exception as e:
    logging.error(f"Error connecting to postgres, error : {e}")

def aggregation_1(engine):
    #vendorid and most used payment_type
    try:
        query="""
            SELECT 
                ny_jan.vendorid, 
                count(ny_jan.tip_amount) AS tip_count, 
                sum(ny_jan.tip_amount) AS sum_of_tips, 
                max(ny_jan.tip_amount), 
                min(ny_jan.tip_amount)
            FROM public."nyc_yellow_2025-01" AS ny_jan
            WHERE ny_jan.tip_amount >= 0
            GROUP BY ny_jan.vendorid 
            ORDER BY sum(ny_jan.tip_amount) DESC;
        """
        agg1 = pd.read_sql(query, engine)
        logging.info("aggregation_1 successful!")
        return agg1

    except Exception as e:
        logging.error(f"Error aggregating! {e}")

def aggregation_2(engine):
    #tip statistics per vendor
    try:
        query="""
            WITH payment_type_ranks AS (
            SELECT ny_jan.vendorid, 
                ny_jan.payment_type, 
                count(*) AS total_trips, 
                DENSE_RANK() OVER (PARTITION BY ny_jan.vendorid ORDER BY count(*)DESC) AS rn
            FROM public."nyc_yellow_2025-01" AS ny_jan
            GROUP BY ny_jan.vendorid, ny_jan.payment_type
            )
            SELECT * 
            FROM payment_type_ranks
            WHERE rn = 1;
        """
        agg2 = pd.read_sql(query, engine)
        logging.info("aggregation_2 successful!")
        return agg2

    except Exception as e:
        logging.error(f"Error aggregating! {e}")


def aggregation_3(engine):
    #Peak hour analysis per day of week
    try:
        query="""
            WITH day_hour_trip_count AS (
            SELECT 
                EXTRACT(DOW FROM tpep_pickup_datetime) AS day_of_week,
                EXTRACT(HOUR FROM ny_jan.tpep_pickup_datetime) AS pickup_hour,
                COUNT(*) AS trip_count
            FROM public."nyc_yellow_2025-01" AS ny_jan
            GROUP BY EXTRACT(DOW FROM tpep_pickup_datetime),EXTRACT(HOUR FROM ny_jan.tpep_pickup_datetime)
            ORDER BY EXTRACT(DOW FROM tpep_pickup_datetime),COUNT(*) DESC
        ),
            ranks AS (
                SELECT
                    *,
                    dense_rank() OVER (PARTITION BY day_of_week ORDER BY trip_count) AS rank_per_count
                FROM 
                    day_hour_trip_count
            )
            SELECT *
            FROM ranks
            WHERE rank_per_count = 1;

        """
        agg3 = pd.read_sql(query, engine)
        logging.info("aggregation_3 successful!")
        return agg3

    except Exception as e:
        logging.error(f"Error aggregating! {e}")

def to_csv(engine):
    try:    
        agg1 = aggregation_1(engine)
        agg2 = aggregation_2(engine)
        agg3 = aggregation_3(engine)

        agg1.to_csv(f"{OUTPUT_DIR}/vendorid_most_used_payment_type.csv", index=False)
        agg2.to_csv(f"{OUTPUT_DIR}/tip_statistics_per vendor.csv", index=False)
        agg3.to_csv(f"{OUTPUT_DIR}/peak_hour_analysis_per_day_of_week.csv", index=False)
        logging.info(f"Saving CSV to {OUTPUT_DIR} sucessful!")

    except Exception as e:
        logging.error(f"Error saving to csv to {OUTPUT_DIR}, error {e}")

def main():
    logging.info("="*80)
    logging.info("Creating aggregations...")
    logging.info("="*80)

    to_csv(engine)

    logging.info("="*80)
    logging.info("Aggregations completed!")
    logging.info("="*80)

if __name__ == "__main__":
    main()