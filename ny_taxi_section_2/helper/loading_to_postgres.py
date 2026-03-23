import logging
import pandas as pd
from datetime import datetime
from psycopg2 import sql, extras
import psycopg2


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


class PostgresConnector:
    def __init__(self, dbname, user, password,  host = "localhost", port = 5432):
        try:
            self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
            self.conn.autocommit = False
            self.cursor = self.conn.cursor()
            logging.info(f"Connected to postgreSQL {dbname}")
        except Exception as e:
            logging.error(f"Error Connecting: {e}")
            raise
    
    def table_exist(self, table_name):
        query = """
        SELECT EXISTS(
            SELECT FROM information_schema.tables
                WHERE table_name = %s
        );
        """

        self.cursor.execute(query, (table_name,))
        exists = self.cursor.fetchone()[0]
        return exists
    
    def create_table(self, table_name, dataframe):
        try:
            columns_def ={}
            for col, dtype in zip(dataframe.columns, dataframe.dtypes):
                if "int" in str(dtype):
                    columns_def[col] = "INT"
                elif "float" in str(dtype):
                    columns_def[col] = "FLOAT"
                elif "datetime" in str(dtype):
                    columns_def[col] = "TIMESTAMP"
                else:
                    columns_def[col] = "TEXT"

            columns_def["load_time"] = "TIMESTAMP DEFAULT NOW()"
            logging.info(f"Data type successfully mapped!")

            query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    {}
                )
                PARTITION BY LIST(run_id);
            """).format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(
                    sql.SQL(f"{col} {dtype}") for col, dtype in columns_def.items()
                )
            )
            self.cursor.execute(query)
            logging.info(f"Table {table_name} created!")

            for run_id in range(1, 8):  # 1=Monday, 7=Sunday (Postgres ISODOW)
                partition_name = f"{table_name}_run_{run_id}"
                create_partition_query = sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {} 
                    PARTITION OF {} 
                    FOR VALUES IN (%s);
                """).format(
                    sql.Identifier(partition_name),
                    sql.Identifier(table_name)
                )
                self.cursor.execute(create_partition_query, (run_id, ))
                logging.info(f"Created partition for run_id {run_id}")

        except Exception as e:
            logging.error(f"Error creating table: {e}")
            raise

    def create_indexes(self, table_name, index_cols):
        # index_cols = ["tpep_pickup_datetime", "pulocationid", "payment_type", "run_id"]
        try:
            for col in index_cols:
                index_name = f"{table_name}_{col}_idx"

                query = sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                    sql.Identifier(index_name),
                    sql.Identifier(table_name),
                    sql.Identifier(col)
                )              

                self.cursor.execute(query)
                logging.info(f"Index created {index_name}")
            self.conn.commit()
            logging.info("All indexes commited to database")
        except Exception as e:
            logging.error(f"Error creating indexes {e}")
            raise

    def insert_table(self, table_name, dataframe, batch_size=5000):
        try:
            if dataframe.empty:
                logging.error(f"Dataframe is empty!")
                return False

            dataframe["load_time"] = datetime.now()

            columns = list(dataframe.columns)
            query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(map(sql.Identifier,columns)),
            )

           #itertuples is faster than looping with iterrows 
            records = list(dataframe.itertuples(index = False, name = None))

            for start in range(0, len(records), batch_size):
               batch = records[start: start + batch_size]
               extras.execute_values(self.cursor, query, batch, page_size = batch_size)
            
            self.conn.commit()
            logging.info(f"Inserted {len(records)} rows into {table_name}")
            return True

        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error inserting table {table_name}: {e}")
            return False   
    
    def close(self):
        try:
            if self.cursor and self.conn:
                self.cursor.close()
                self.conn.close()
                logging.info(f"Connection to Postgres closed")
            
            else:
                logging.error("No connection Established")

        except Exception as e:
           logging.error(f"Error:{e}")

    def load(self, table_name, dataframe):

        index_cols = ["tpep_pickup_datetime", "pulocationid", "payment_type", "run_id"]

        if not self.conn:
            logging.info(f"Cannot connect to Postgres!")
            return False
        
        try:                 
            if not self.table_exist(table_name):
                logging.info(f"table {table_name} does not exists - creating...")
                
                self.create_table(table_name, dataframe)
                logging.info(f"{table_name} table created successfully")

                self.create_indexes(table_name, index_cols) 

            self.insert_table(table_name, dataframe)
            return True

        except Exception as e:
            print(f"Error creating/loading {table_name}: {e}")
            self.conn.rollback()
            return False
    
   