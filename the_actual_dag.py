from airflow import models
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
from requests import Session
import pandas as pd
import postgres
import sys
import time

YAHOO_FINANCE_API_KEY = "cbb9ea0430msh6efe1cb28bb8201p19401fjsn7a8d4adfc565"
YAHOO_FINANCE_HOST = "yahoo-finance-real-time1.p.rapidapi.com"
BASE_URL = "https://yahoo-finance-real-time1.p.rapidapi.com/"
IS_PROD = False # Default to False

# Put in existing schema
schemaName = 'financial'
tables = ['stockchart', 'stockquote', 'indexchart']
tickers = ["AIK-B.ST", "MANU", "AAB.CO", "AGF-B.CO", "PARKEN.CO", "BIF.CO", "CCP.L", "BVB.DE", "AJAX.AS", "JUVE.MI", "SSL.MI", "FCP.LS", "SLBEN.LS", "SCP.LS", "FENER.IS", "GSRAY.IS", "BJKAS.IS", "TSPOR.IS"]
indexes = ["^OMX", "^OMXC25", "^DJUS", "^FTSE", "^GDAXI", "^AEX", "FTSEMIB.MI", "PSI20.LS", "XU100.IS"]
clubIdMapping = {
    "AIK-B.ST": 9185,
    "MANU": 2188, 
    "AAB.CO": 31088, 
    "AGF-B.CO": 31974, 
    "PARKEN.CO": 31402, 
    "BIF.CO": 31169, 
    "CCP.L": 671, 
    "BVB.DE": 31914, 
    "AJAX.AS": 15623, 
    "JUVE.MI": 21376, 
    "SSL.MI": 21439, 
    "FCP.LS": 13421, 
    "SLBEN.LS": 13031, 
    "SCP.LS": 13547, 
    "FENER.IS": 8082, 
    "GSRAY.IS": 8094, 
    "BJKAS.IS": 7951, 
    "TSPOR.IS": 8444
}

# Default Airflow task arguments
default_args = {
    'email': ['carl@goalunit.com'],
    'email_on_failure': True,
    'retries': 0,
    'depends_on_past': False,
    'start_date': days_ago(1),
    'catchup': False,
}

# If the DataFrame is empty, skips the upload.
def uploadToDB(df: pd.DataFrame, table_name: str, merge_query: str):
        if df.empty:
            print(f"{datetime.now()}: Response recieved for {table_name} was empty. Skipping upload...")
            return

        postgres.merge_to_postgres(df, merge_query, is_prod=IS_PROD)
        print("{} Loaded Yahoo Finance data to {}".format(datetime.now(), table_name))
        
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
        # Om vi gjort 2 försök -> avbryt
        if(attemptCount >= 2):
            print("2 attempts were made, moving on with the next request.")
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
    
    if tableName in ['stockchart', 'indexchart']:
        conflictColumns = ['symbol', 'timestamp']
        conflictTarget = ", ".join(conflictColumns)
        
        mergeQuery = f"""
        INSERT INTO {schemaName}.{tableName} ({columnsToInsert})
        VALUES ({valuePlaceholders})
        ON CONFLICT ({conflictTarget}) 
        DO NOTHING;
        """
    elif tableName == 'stockquote':
        mergeQuery = f"""
        INSERT INTO {schemaName}.{tableName} ({columnsToInsert})
        VALUES ({valuePlaceholders});
        """
    else:
        raise ValueError(f"Unknown table: {tableName}")
    return mergeQuery

# -----------------------------------------------------------------  #  
# --- Get stock data for chosen ticker within range and interval --- #
# -----------------------------------------------------------------  #  
def getStockChartData(ticker, withinRange, interval, session):
    url = f"{BASE_URL}/stock/get-chart?symbol={ticker}&lang=en-US&useYfid=true&includeAdjustedClose=true&events=div%2Csplit%2Cearn&range={withinRange}&interval={interval}"
    res = Request(url, session=session)

    if res is None or res.empty or 'chart' not in res or 'result' not in res['chart']:
        print(f"No valid data found for {ticker}.")
        return None

    chartData = res['chart']['result'][0]
    timestamps = chartData.get('timestamp', [])
    quote = chartData.get('indicators', {}).get('quote', [])[0]
    adjclose = chartData.get('indicators', {}).get('adjclose', [])[0]

    if not timestamps or not quote:
        print(f"No valid quote data for {ticker}.")
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
                'adjclose': adjclose['adjclose'][i],
                'volume': quote['volume'][i],
            })

    if not valid_data:
        print(f"No valid data found for {ticker}.")
        return None

    df = pd.DataFrame(valid_data)
    df["symbol"] = ticker
    df["clubid"] = clubIdMapping.get(ticker, None)
    return df[['timestamp', 'high', 'low', 'open', 'close', 'adjclose', 'volume', 'symbol', 'clubid']]


# -----------------------------------------------------------------  #  
# ---- Get stock quotes, general info, can take max 200 tickers ---- #
# -----------------------------------------------------------------  #  
def getStockQuotes(tickersToGet, session):
    tickerQuery = "%2C".join(tickersToGet)
    url = f"{BASE_URL}/market/get-quotes?region=US&symbols={tickerQuery}"
    res = Request(url, session=session)

    if res is None or res.empty or 'quoteResponse' not in res or 'result' not in res.get('quoteResponse', {}):
        print("Monthly stock quotes could not be found.")
        return None
    resultData = res['quoteResponse']['result']
    if not resultData:
        print("No result data found for monthly quotes.")
        return None
    
    df = pd.DataFrame(resultData)
    df["clubid"] = df["symbol"].map(clubIdMapping)
    df["timestamp"] = int(datetime.now().timestamp())
    return df[['symbol', 'shortName', 'timestamp', 'regularMarketPrice', 'marketCap', 'currency', 'financialCurrency', 'exchangeTimezoneShortName', 'exchange', 'fullExchangeName', 'gmtOffSetMilliseconds', 'sharesOutstanding', 'beta', 'bookValue', 'priceToBook', 'longName', 'clubid']]

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
def fecthQuotesAndUploadToDB(tickers, session):
        quotes = getStockQuotes(tickers, session)
        if quotes is None or quotes.empty:
            print("No stock quotes retrieved. Skipping upload...")
            return
          
        quoteTableName = 'stockquote'
        quotesMQ = generateMergeQuery(quotes, quoteTableName)
        uploadToDB(quotes, quoteTableName, quotesMQ)

def fetchChartsAndUploadToDB(session, range, interval):
        for ticker in tickers: 
            chart = getStockChartData(ticker, range, interval, session)
            if chart is None or chart.empty:
                print(f"No chart data retrieved for {ticker}. Skipping upload...")
                continue
              
            chartTableName = 'stockchart'
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
    postgres.run_sql_query(f"CREATE SCHEMA IF NOT EXISTS {schemaName};", commit_changes=True, is_prod=IS_PROD)

def createTablesIfNotExists():
    for table in tables:
        if table == 'stockchart':
            postgres.run_sql_query(f"""
                CREATE TABLE IF NOT EXISTS {schemaName}.{table} (
                    stockchartid SERIAL PRIMARY KEY,
                    timestamp BIGINT,
                    high NUMERIC(18,2),
                    low NUMERIC(18,2),
                    open NUMERIC(18,2),
                    close NUMERIC(18,2),
                    adjclose NUMERIC(18,2),
                    volume INTEGER,
                    symbol VARCHAR(10),
                    clubid INT,
                    CONSTRAINT uc_timestamp_symbol UNIQUE (symbol, timestamp),
                    CONSTRAINT fk_club_stockquote FOREIGN KEY (clubid) REFERENCES club.club(clubid)
                );
            """, commit_changes=True, is_prod=IS_PROD)
        if table == 'stockquote':
            postgres.run_sql_query(f"""
                CREATE TABLE IF NOT EXISTS {schemaName}.{table} (
                    stockquoteid SERIAL PRIMARY KEY,
                    symbol VARCHAR(10),
                    shortName TEXT,
                    clubid INT, 
                    marketcap BIGINT,
                    currency VARCHAR(10),
                    financialCurrency VARCHAR(10),
                    exchangeTimezoneShortName VARCHAR(10),
                    exchange VARCHAR(10),
                    fullExchangeName TEXT,
                    gmtOffSetMilliseconds INT,
                    sharesOutstanding BIGINT,
                    beta NUMERIC(18,2),
                    bookValue NUMERIC(18,2),
                    priceToBook NUMERIC(18,2),
                    longName TEXT,
                    regularMarketPrice NUMERIC(18,2),
                    timestamp BIGINT, 
                    CONSTRAINT fk_club_stockchart FOREIGN KEY (clubid) REFERENCES club.club(clubid)
                );
            """, commit_changes=True, is_prod=IS_PROD)
        if table == 'indexchart':
            postgres.run_sql_query(f"""
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
            """, commit_changes=True, is_prod=IS_PROD)

# -----------------------------------------------------------------  #  
# ---------------- Main function - called in pipeline -------------- #
# -----------------------------------------------------------------  #  
def integrationYahooFinance(is_prod: bool, **kwargs): 
    with Session() as session:
        session.headers.update({
            "x-rapidapi-key": YAHOO_FINANCE_API_KEY,
            "x-rapidapi-host": YAHOO_FINANCE_HOST
        })
        global IS_PROD
        IS_PROD = is_prod
        createSchemaIfNotExists()
        createTablesIfNotExists()

        # TODO: Om hämta historisk daglig data, sätt withinRange till "10y"
        withinRange = "5d"
      
        # Run daily
        fetchChartsAndUploadToDB(session, withinRange, "1d")
        fetchIndexChartsAndUploadToDB(session, withinRange, "1d")

        # Run monthly
        currentQuoteData = postgres.run_sql_query(f"SELECT * FROM {schemaName}.stockquote;", commit_changes=False, is_prod=IS_PROD)
        if currentQuoteData.empty:
            print("No data in stockquote, fetching...")
            fecthQuotesAndUploadToDB(tickers, session)
        else:
            currentQuoteData['timestamp'] = pd.to_datetime(currentQuoteData['timestamp'], unit="s")
            latestTimestamp = currentQuoteData["timestamp"].max()
            oneMonthAgo = datetime.now() - timedelta(days=30)
            if latestTimestamp < oneMonthAgo:
                print("stockquote data older than a month, fetching...")
                fecthQuotesAndUploadToDB(tickers, session)
            else:
                print("stockquote data is fetched within this month...")


# ---------------------------------------------
# DEV DAG (Non-Production)
# ---------------------------------------------
with models.DAG(
    dag_id="yahoo-finance-pipeline-dev",
    description="Yahoo Finance DAG for Development",
    schedule='@daily',
    start_date=datetime(2024, 2, 1),
    catchup=False,
    tags=["yahoo-finance", "dev"],
) as dag_dev:

    Start_dev = BashOperator(
        task_id="START_PIPELINE_DEV",
        bash_command='echo "START PIPELINE (DEV)"; sleep 15',
        dag=dag_dev,
    )

    YahooFinance_dev = PythonOperator(
        task_id='YAHOO_FINANCE_GENERAL_DEV',
        python_callable=integrationYahooFinance,
        op_kwargs={'is_prod': False},  # Dev mode
        dag=dag_dev
    )

    End_dev = BashOperator(
        task_id="END_PIPELINE_DEV",
        bash_command='echo "PIPELINE ENDED (DEV)"; sleep 15',
        dag=dag_dev,
        trigger_rule="all_done"
    )

    Start_dev >> YahooFinance_dev >> End_dev

# ---------------------------------------------
# PROD DAG (Production)
# ---------------------------------------------
with models.DAG(
    dag_id="yahoo-finance-pipeline-prod",
    description="Yahoo Finance DAG for Production",
    schedule='@daily',
    start_date=datetime(2024, 2, 1),
    catchup=False,
    tags=["yahoo-finance", "prod"],
) as dag_prod:

    Start_prod = BashOperator(
        task_id="START_PIPELINE_PROD",
        bash_command='echo "START PIPELINE (PROD)"; sleep 15',
        dag=dag_prod,
    )

    YahooFinance_prod = PythonOperator(
        task_id='YAHOO_FINANCE_GENERAL_PROD',
        python_callable=integrationYahooFinance,
        op_kwargs={'is_prod': True},  # Prod mode
        dag=dag_prod
    )

    End_prod = BashOperator(
        task_id="END_PIPELINE_PROD",
        bash_command='echo "PIPELINE ENDED (PROD)"; sleep 15',
        dag=dag_prod,
        trigger_rule="all_done"
    )

    Start_prod >> YahooFinance_prod >> End_prod