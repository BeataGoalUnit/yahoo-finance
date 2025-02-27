from datetime import datetime
from requests import Session
import pandas as pd
from local_db_connection import merge_to_postgres
from local_db_connection import run_sql_query
import sys
import time
from datetime import datetime, timedelta

YAHOO_FINANCE_API_KEY = "137f78e0f7msh62e445a737cf689p109e79jsne0788c058c05" 
YAHOO_FINANCE_HOST = "yahoo-finance-real-time1.p.rapidapi.com"
BASE_URL = "https://yahoo-finance-real-time1.p.rapidapi.com/"

# Put in existing schema
schemaName = 'financial'
tables = ['exchangechart', 'indexchart']

indexes = ["^OMX", "^OMXC25", "^DJUS", "^FTSE", "^GDAXI", "^AEX", "FTSEMIB.MI", "PSI20.LS", "XU100.IS"]
currencies = [
    "SEK",
    "DKK",
    "USD",
    "GBP",
    "EUR",
    "TRY",
]

# -----------------------------------------------------------------  #             
# ------------------ Upload to DB ---------------------------------- #
# -----------------------------------------------------------------  #  
def uploadToDB(df: pd.DataFrame, tableName: str, mergeQuery: str):
    if df.empty:
        print(f"{datetime.now()}: Response recieved for {tableName} was empty. Skipping upload...")
        return
    merge_to_postgres(df, mergeQuery)
    print("{} Loaded Yahoo Finance data to {}".format(datetime.now(), tableName))
    
# -----------------------------------------------------------------  #             
# Makes an API request and handles errors with retries if necessary. #
# -----------------------------------------------------------------  #  
def Request(url: str, session, attempt: int = 0):
    response = session.get(url)

    # ---------------------- #
    # Request error handling #
    # ---------------------- #
    if(response.status_code == 200):
        res = response.json()
        return pd.DataFrame(res)
    
    attemptCount = attempt + 1

    # Endpoint does not exist
    if(response.status_code == 400 or response.status_code == 404):
        print(f"{response.status_code} {response.reason}. Skipping.")

        return None

    # If "Too Many Requests" error, pause for 1 second and then try again. Limit: 5 requests/second
    if(response.status_code == 429): 
        # Om vi gjort 5 försök -> avbryt
        if(attemptCount >= 2):
            print("10 attempts were made, moving on with the next request.")
            attemptCount = 0
            return None
        # Retry if issue is too many requests per sec
        print(f"429: Too many requests, attempt: [{attemptCount}] retrying...")
        time.sleep(1)
        return Request(url=url, session=session, attempt=attemptCount)
    
    # If "Internal Server Error", pause for an hour and then try again.
    if(response.status_code == 500):
        # Om vi gjort 10 försök -> avbryt
        if(attemptCount >= 10):
            print("10 attempts were made, moving on with the next request.")
            attemptCount = 0
            return None

        print(f"500: Internal Server Error, attempt: [{attemptCount}], pausing for 10 minutes and then retrying...")
        time.sleep(600)

        return Request(url=url, session=session, attempt=attemptCount)
    
    # If "Service Unavailable 503" error, pause for 2 minutes and then try again.
    if(response.status_code == 503):
        # Om vi gjort 10 försök -> avbryt
        if(attemptCount >= 10):
            print("Aborting")
            sys.exit(1)

        print(f"503: Service unavailable, attempt: [{attemptCount}], retrying...")
        time.sleep(120)

        return Request(url=url, session=session, attempt=attemptCount)
    
    # If "Gateway Time-out" error, pause for 30 seconds and then try again (internet access issues).
    if(response.status_code == 504): 
        print(f"504: Gateway time-out, attempt: [{attemptCount}], retrying...")
        time.sleep(30)

        return Request(url=url, attempt=attemptCount)
    
    print(f"When calling {url}: An unexpected error occured: {response.status_code} {response.reason}. More info: {response.text}")
    return None

# -----------------------------------------------------------------  #  
# ------------------- Generates a merge query ---------------------  #  
# -----------------------------------------------------------------  #  
def generateMergeQuery(df: pd.DataFrame, tableName: str):
    columns = df.columns.tolist()
    valuePlaceholders = ", ".join(["%s"] * len(columns))
    columnsToInsert = ", ".join(columns)
    
    if tableName == 'exchangechart':
        conflictColumns = ['fromcurrency', 'tocurrency', 'dateutc']
        conflictTarget = ", ".join(conflictColumns)
        
        mergeQuery = f"""
        INSERT INTO {schemaName}.{tableName} ({columnsToInsert})
        VALUES ({valuePlaceholders})
        ON CONFLICT ({conflictTarget}) 
        DO NOTHING;
        """
    elif tableName == 'indexchart':
        conflictColumns = ['symbol', 'timestamp']
        conflictTarget = ", ".join(conflictColumns)
        
        mergeQuery = f"""
        INSERT INTO {schemaName}.{tableName} ({columnsToInsert})
        VALUES ({valuePlaceholders})
        ON CONFLICT ({conflictTarget}) 
        DO NOTHING;
        """
    else:
        raise ValueError(f"Unknown table: {tableName}")
    return mergeQuery

# -----------------------------------------------------------------  #  
# --- Get stock data for chosen ticker within range and interval --- #
# -----------------------------------------------------------------  #  
def getExchangeToEurData(currency, withinRange, interval, session):
    url = f"{BASE_URL}/stock/get-chart?symbol={currency}EUR%3DX&range={withinRange}&interval={interval}"
    res = Request(url, session=session)

    if res is None or res.empty or 'chart' not in res or 'result' not in res['chart']:
        print(f"No valid data found for {currency}.")
        return None

    chartData = res['chart']['result'][0]
    timestamps = chartData.get('timestamp', [])
    quote = chartData.get('indicators', {}).get('quote', [])[0]

    if not timestamps or not quote:
        print(f"No valid data for {currency}.")
        return None

    # Iterate through all the timestamps and filter out invalid data
    valid_data = []
    for i in range(len(timestamps)):
        if quote['close'][i] is not None:
            valid_data.append({
                'timestamp': timestamps[i],
                'rate': quote['close'][i],
            })

    if not valid_data:
        print(f"No valid data found for {currency}.")
        return None

    df = pd.DataFrame(valid_data)
    df["fromcurrency"] = currency
    df["tocurrency"] = "EUR"
    df["dateutc"] = pd.to_datetime(df["timestamp"], unit='s', utc=True).dt.date
    return df[['rate', 'dateutc', 'fromcurrency', 'tocurrency']]

# -----------------------------------------------------------------  #  
# --- Get index data for chosen ticker within range and interval --- #
# -----------------------------------------------------------------  # 
def getIndexChartData(index, withinRange, interval, session):
    url = f"{BASE_URL}/stock/get-chart?symbol={index}&range={withinRange}&interval={interval}"
    res = Request(url, session=session)

    if res is None or res.empty or 'chart' not in res or 'result' not in res['chart']:
        print(f"No valid data found for {index}.")
        return None

    indexData = res['chart']['result'][0]
    timestamps = indexData.get('timestamp', [])
    meta = indexData.get('meta', {})
    quote = indexData.get('indicators', {}).get('quote', [])[0]

    if not timestamps or not quote:
        print(f"No valid quote data for {index}.")
        return None

    # Iterate through all the timestamps and filter out invalid data
    valid_data = []
    for i in range(len(timestamps)):
        if quote['close'][i] is not None:  # Only include data where 'close' value is valid
            valid_data.append({
                'timestamp': timestamps[i],
                'high': quote['high'][i],
                'low': quote['low'][i],
                'open': quote['open'][i],
                'close': quote['close'][i],
            })

    if not valid_data:
        print(f"No valid data found for {index}.")
        return None

    df = pd.DataFrame(valid_data)
    df["symbol"] = index
    df["dateutc"] = pd.to_datetime(df["timestamp"], unit='s', utc=True).dt.date
    df["shortname"] = meta.get('shortName')
    df["currency"] = meta.get('currency')
    return df[['timestamp', 'shortname', 'currency', 'high', 'low', 'open', 'close', 'dateutc', 'symbol']]

# -----------------------------------------------------------------  #  
# ---------- Wrappers for fetch data and upload to DB -------------- #
# -----------------------------------------------------------------  #  
def fetchExchangeChartsAndUploadToDB(session, range, interval):
    for currency in currencies: 
        chart = getExchangeToEurData(currency, range, interval, session)
        if chart is None or chart.empty:
            print(f"No chart data retrieved for exchange. Skipping upload...")
            continue
        chartTableName = 'exchangechart'
        chartsMQ = generateMergeQuery(chart, chartTableName)
        uploadToDB(chart, chartTableName, chartsMQ)   

def fetchIndexChartsAndUploadToDB(session, range, interval):
    for index in indexes:
        chart = getIndexChartData(index, range, interval, session)
        if chart is None or chart.empty:
            print(f"No chart data retrieved for {index}. Skipping upload...")
            continue
        chartTableName = 'indexchart'
        chartsMQ = generateMergeQuery(chart, chartTableName)
        uploadToDB(chart, chartTableName, chartsMQ)   


# -----------------------------------------------------------------  #  
# --- Checks if schema and / or tables exists - else creates ------- #
# -----------------------------------------------------------------  #  
def createSchemaIfNotExists():
    run_sql_query(f"CREATE SCHEMA IF NOT EXISTS {schemaName};", commit_changes=True)

def createTablesIfNotExists():
    for table in tables:
        if table == 'exchangechart':
            run_sql_query(f"""
                CREATE TABLE IF NOT EXISTS {schemaName}.{table} (
                    exchangechartid SERIAL PRIMARY KEY,
                    rate NUMERIC(18,2),
                    fromcurrency VARCHAR(10),
                    tocurrency VARCHAR(10),
                    dateutc DATE,
                    CONSTRAINT uc_dateutc_currency UNIQUE (fromcurrency, tocurrency, dateutc)
                );
            """, commit_changes=True)
        if table == 'indexchart':
            run_sql_query(f"""
                CREATE TABLE IF NOT EXISTS {schemaName}.{table} (
                    indexchartid SERIAL PRIMARY KEY,
                    timestamp BIGINT,
                    currency VARCHAR(10),
                    shortName TEXT,
                    high NUMERIC(18,2),
                    low NUMERIC(18,2),
                    open NUMERIC(18,2),
                    close NUMERIC(18,2),
                    symbol VARCHAR(10),
                    dateutc DATE,
                    CONSTRAINT uc_symbol_timestamp UNIQUE (symbol, timestamp)
                );
            """, commit_changes=True)
    
# -----------------------------------------------------------------  #  
# ---------------- Main function - called in pipeline -------------- #
# -----------------------------------------------------------------  #  
def integrationYahooFinance(): 
    with Session() as session:
        session.headers.update({
            "x-rapidapi-key": YAHOO_FINANCE_API_KEY,
            "x-rapidapi-host": YAHOO_FINANCE_HOST
        })

        createSchemaIfNotExists()
        createTablesIfNotExists()

        # TODO: Change middle argument (range) to "10y" for initial load
        # fetchChartsAndUploadToDB(session, "10y", "1d")
        withinRange = "5d"
        fetchIndexChartsAndUploadToDB(session, withinRange, "1d")
        fetchExchangeChartsAndUploadToDB(session, "5d", "1d")

integrationYahooFinance()
