import psycopg2
import logging
import pandas as pd 
from contextlib import contextmanager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Local PostgreSQL connection settings
LOCAL_DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "beata",
    "user": "beata",
    "password": "newpassword"
}

@contextmanager
def get_db_connection():
    """Context manager for local PostgreSQL connection."""
    conn = None
    try:
        logging.info(f"Connecting to local PostgreSQL database at {LOCAL_DB_CONFIG['host']}:{LOCAL_DB_CONFIG['port']}, "
                     f"database '{LOCAL_DB_CONFIG['dbname']}' as user '{LOCAL_DB_CONFIG['user']}'.")
        
        conn = psycopg2.connect(**LOCAL_DB_CONFIG)
        logging.info("Database connection established.")
        yield conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

def merge_to_postgres(df, merge_query: str, batch_size: int = 5000):
    """Merge DataFrame into local PostgreSQL using a provided merge query, with batch processing."""
    if df.empty:
        logging.warning("The DataFrame is empty. No rows to merge.")
        return

    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            # Convert DataFrame to standard Python data types
            df = df.astype(object).where(pd.notnull(df), None)

            # Convert DataFrame to list of tuples
            data_tuples = [tuple(row) for row in df.values]

            # Insert in batches
            for i in range(0, len(data_tuples), batch_size):
                batch = data_tuples[i:i + batch_size]
                cursor.executemany(merge_query, batch)
                logging.info(f"Batch {i // batch_size + 1} inserted successfully.")

            # Commit the transaction
            conn.commit()
            logging.info(f"Successfully merged {len(df)} rows into the database.")
        except Exception as e:
            conn.rollback()
            logging.error(f"Failed to execute merge query: {e}")
            raise
        finally:
            cursor.close()

def call_sp(sp: str):
    """Execute a stored procedure in the local PostgreSQL database."""
    with get_db_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(f'CALL {sp}')
            conn.commit()
            logging.info(f"Stored procedure {sp} executed successfully.")
        except Exception as e:
            conn.rollback()
            logging.error(f"Failed to execute stored procedure: {e}")
            raise
        finally:
            cur.close()

def run_sql_query(query: str, commit_changes: bool = False):
    """Execute a SQL query. Commit changes false for SELECT."""
    with get_db_connection() as conn:
        try:
            cur = conn.cursor()
            cur.execute(query)

            if not commit_changes:
                results = cur.fetchall()
                column_names = [desc[0] for desc in cur.description]
                df = pd.DataFrame(results, columns=column_names)
                logging.info(f"Query executed successfully. {len(df)} rows returned.")
                return df
            else:
                conn.commit()
                logging.info("Query executed and committed successfully.")
        except Exception as e:
            logging.error(f"Failed to execute query: {e}")
            conn.rollback()  # 🔄 Roll back on failure
            raise
        finally:
            cur.close()