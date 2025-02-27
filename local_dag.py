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
tables = ['stockchart', 'stockquote']

tickers = ["AIK-B.ST", "MANU", "AAB.CO", "AGF-B.CO", "PARKEN.CO", "BIF.CO", "CCP.L", "BVB.DE", "AJAX.AS", "JUVE.MI", "SSL.MI", "FCP.LS", "SLBEN.LS", "SCP.LS", "FENER.IS", "GSRAY.IS", "BJKAS.IS", "TSPOR.IS"]

clubDataMapping = {
    "AIK-B.ST": {"clubid": 9185, "indexSymbol": "^OMX"},
    "MANU": {"clubid": 2188, "indexSymbol": "^DJUS"},
    "AAB.CO": {"clubid": 31088, "indexSymbol": "^OMXC20"},
    "AGF-B.CO": {"clubid": 31974, "indexSymbol": "^OMXC20"},
    "PARKEN.CO": {"clubid": 31402, "indexSymbol": "^OMXC20"},
    "BIF.CO": {"clubid": 31169, "indexSymbol": "^OMXC20"},
    "CCP.L": {"clubid": 671, "indexSymbol": "^FTSE"},
    "BVB.DE": {"clubid": 31914, "indexSymbol": "^GDAXI"},
    "AJAX.AS": {"clubid": 15623, "indexSymbol": "^AEX"},
    "JUVE.MI": {"clubid": 21376, "indexSymbol": "FTSEMIB.MI"},
    "SSL.MI": {"clubid": 21439, "indexSymbol": "FTSEMIB.MI"},
    "FCP.LS": {"clubid": 13421, "indexSymbol": "PSI20.LS"},
    "SLBEN.LS": {"clubid": 13031, "indexSymbol": "PSI20.LS"},
    "SCP.LS": {"clubid": 13547, "indexSymbol": "PSI20.LS"},
    "FENER.IS": {"clubid": 8082, "indexSymbol": "XU100.IS"},
    "GSRAY.IS": {"clubid": 8094, "indexSymbol": "XU100.IS"},
    "BJKAS.IS": {"clubid": 7951, "indexSymbol": "XU100.IS"},
    "TSPOR.IS": {"clubid": 8444, "indexSymbol": "XU100.IS"},
}

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
    # TODO: ADD A COUNTER HERE!!!! if monthly limit is reached
    if(response.status_code == 429): 
        print("429: Too many requests, retrying...")
        time.sleep(1)

        return Request(url=url, session=session)
    
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
    
    if tableName == 'stockchart':
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
    df["clubid"] = df["symbol"].map(lambda ticker: clubDataMapping.get(ticker, {}).get("clubid", None))
    df["dateutc"] = pd.to_datetime(df["timestamp"], unit='s', utc=True).dt.date
    return df[['timestamp', 'high', 'low', 'open', 'close', 'adjclose', 'volume', 'dateutc', 'symbol', 'clubid']]

# -----------------------------------------------------------------  #  
# ---- Get stock quotes, general info, can take max 200 tickers ---- #
# -----------------------------------------------------------------  #  
def getClubStockQuotes(session):
    tickerQuery = "%2C".join(tickers)
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
    df["clubid"] = df["symbol"].map(lambda ticker: clubDataMapping.get(ticker, {}).get("clubid", None))
    df["compareIndex"] = df["symbol"].map(lambda ticker: clubDataMapping.get(ticker, {}).get("indexSymbol", None))

    df["timestamp"] = int(datetime.now().timestamp())
    df["dateutc"] = pd.to_datetime(df["timestamp"], unit='s', utc=True).dt.date

    return df[['symbol', 'shortName', 'timestamp', 'regularMarketPrice', 'marketCap', 'currency', 'financialCurrency', 'exchangeTimezoneShortName', 'exchange', 'fullExchangeName', 'gmtOffSetMilliseconds', 'sharesOutstanding', 'beta', 'bookValue', 'priceToBook', 'longName', 'clubid', 'dateutc', 'compareIndex' ]]

# -----------------------------------------------------------------  #  
# ---------- Wrappers for fetch data and upload to DB -------------- #
# -----------------------------------------------------------------  #  
def fecthQuotesAndUploadToDB(session):
        quotes = getClubStockQuotes(session)
        quoteTableName = 'stockquote'
        quotesMQ = generateMergeQuery(quotes, quoteTableName)
        uploadToDB(quotes, quoteTableName, quotesMQ)

def fetchChartsAndUploadToDB(session, range, interval):
    for ticker in tickers: 
        chart = getStockChartData(ticker, range, interval, session)
        chartTableName = 'stockchart'
        chartsMQ = generateMergeQuery(chart, chartTableName)
        uploadToDB(chart, chartTableName, chartsMQ)   


# -----------------------------------------------------------------  #  
# --- Checks if schema and / or tables exists - else creates ------- #
# -----------------------------------------------------------------  #  
def createSchemaIfNotExists():
    run_sql_query(f"CREATE SCHEMA IF NOT EXISTS {schemaName};", commit_changes=True)

def createTablesIfNotExists():
    for table in tables:
        if table == 'stockchart':
            run_sql_query(f"""
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
                    dateutc DATE,
                    CONSTRAINT uc_timestamp_symbol UNIQUE (symbol, timestamp),
                    CONSTRAINT fk_club_stockquote FOREIGN KEY (clubid) REFERENCES club.club(clubid)
                );
            """, commit_changes=True)
        if table == 'stockquote':
            run_sql_query(f"""
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
                    dateutc DATE,
                    compareindex VARCHAR(10),
                    CONSTRAINT fk_club_stockchart FOREIGN KEY (clubid) REFERENCES club.club(clubid)
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

        # TODO: Run to fetch all daily historical stock chart data
        # fetchChartsAndUploadToDB(session, "10y", "1d")

        # Run daily
        fetchChartsAndUploadToDB(session, "5d", "1d")

        # Run monthly
        currentQuoteData = run_sql_query(f"SELECT * FROM {schemaName}.stockquote;")
        if currentQuoteData.empty:
            print("No data in stockquote, fetching...")
            fecthQuotesAndUploadToDB(session)
        else:
            currentQuoteData['timestamp'] = pd.to_datetime(currentQuoteData['timestamp'], unit="s")
            latestTimestamp = currentQuoteData["timestamp"].max()
            oneMonthAgo = datetime.now() - timedelta(days=30)
            if latestTimestamp < oneMonthAgo:
                print("stockquote data older than a month, fetching...")
                fecthQuotesAndUploadToDB(session)
            else:
                print("stockquote data is fetched within this month...")

integrationYahooFinance()
