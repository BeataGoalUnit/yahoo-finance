from airflow.hooks.base import BaseHook
from contextlib import contextmanager
import psycopg2
import logging
import pandas as pd 

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@contextmanager
def get_db_connection(conn_id):
    """Context manager for PostgreSQL connection using psycopg2 with parameters from Airflow connection."""
    conn = None
    try:
        # Fetch the connection parameters from Airflow
        connection = BaseHook.get_connection(conn_id)
        host = connection.host
        port = connection.port or 5432
        dbname = connection.schema
        user = connection.login
        password = connection.password

        # Log the connection details (excluding password)
        logging.info(f"Connecting to PostgreSQL database at {host}:{port}, database '{dbname}' as user '{user}'.")

        # Establish the connection
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        logging.info("Database connection established.")
        yield conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

def ping():
    logging.info("Starting ping function")
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT * FROM player.player;')
            result = cursor.fetchone()
            print("Ping", result)
            logging.info(f"Ping result: {result}")

def merge_to_postgres(df, merge_query: str, batch_size: int = 5000, is_prod: bool = True):
    """Merge DataFrame into PostgreSQL using a provided merge query, with batch processing."""
    if df.empty:
        logging.warning("The DataFrame is empty. No rows to merge.")
        return

    if is_prod: 
        conn_id = "SQL_Proxy_Prod"
    else: 
        conn_id = "SQL_Proxy_Dev"

    with get_db_connection(conn_id) as conn:
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

def call_sp(sp: str, is_prod: bool = True):
    if is_prod: 
        conn_id = "SQL_Proxy_Prod"
    else: 
        conn_id = "SQL_Proxy_Dev"
   
    with get_db_connection(conn_id) as conn:
        cur = conn.cursor()
        try:
            cur.execute(f'CALL {sp}')
            conn.commit()
        except Exception as e:
                conn.rollback()
                logging.error(f"Failed to execute sp: {e}")
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
            conn.rollback()
            raise
        finally:
            cur.close()