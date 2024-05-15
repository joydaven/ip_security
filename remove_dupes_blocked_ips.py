from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# MySQL connection string for 'cloaca' database
cloaca_connection_string = 'mysql+pymysql://x:x@localhost:3306/cloaca'

# Create database engine for cloaca
cloaca_engine = create_engine(cloaca_connection_string, echo=False)

def delete_duplicate_ips():
    try:
        # SQL query adjusted for 'BlockedID' as the primary key
        delete_query = text("""
            DELETE b1 FROM BlockedIPs b1
            INNER JOIN (
                SELECT MIN(BlockedID) as BlockedID, IP
                FROM BlockedIPs
                GROUP BY IP
                HAVING COUNT(*) > 1
            ) b2 ON b1.IP = b2.IP AND b1.BlockedID > b2.BlockedID;
        """)

        with cloaca_engine.begin() as conn:
            result = conn.execute(delete_query)
            print(f"Deleted {result.rowcount} duplicate rows from BlockedIPs.")

    except SQLAlchemyError as e:
        print(f"An error occurred during the deletion process: {e}")

if __name__ == "__main__":
    delete_duplicate_ips()
    print("Duplicate deletion process completed.")
