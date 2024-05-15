from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import datetime

# MySQL connection strings
scandb_connection_string = 'mysql+pymysql://x:x@localhost:3306/scandb'
cloaca_connection_string = 'mysql+pymysql://x:x@localhost:3306/cloaca'

# Create database engines
scandb_engine = create_engine(scandb_connection_string, echo=False)
cloaca_engine = create_engine(cloaca_connection_string, echo=False)

def process_unique_ips_chunked():
    try:
        # Define your chunk size
        chunksize = 10000  # Adjust based on your system's capability

        # SQL query to fetch unique IPs
        query = "SELECT DISTINCT ip FROM scan_results;"

        with scandb_engine.connect() as conn:
            for chunk in pd.read_sql_query(query, conn, chunksize=chunksize):
                # Prepare the chunk for insertion
                chunk['CampaignID'] = 0
                chunk['BlockReason'] = 'Q'
                chunk['Timestamp'] = datetime.datetime.now()

                # Insert data in chunks into the cloaca database
                with cloaca_engine.begin() as transaction_conn:
                    chunk.to_sql('BlockedIPs', transaction_conn, if_exists='append', index=False, method='multi')
                print("Inserted a chunk into BlockedIPs.")

    except SQLAlchemyError as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    process_unique_ips_chunked()
    print("Data processing completed.")