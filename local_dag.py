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

tables = ['stockchart', 'stockquote']
tickers = ["AIK-B.ST", "MANU"]

clubIdMapping = {
    "AIK-B.ST": 1,
    "MANU": 2 
}

# -----------------------------------------------------------------  #             
# ------------------ Upload to DB ---------------------------------- #
# -----------------------------------------------------------------  #  
def uploadToDB(df: pd.DataFrame, tableName: str, mergeQuery: str):
    if df.empty:
        print(f"{datetime.now()}: Response recieved for {tableName} was empty. Skipping upload...")
        return
    merge_to_postgres(df, mergeQuery, len(df.columns))
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
        conflictColumns = ['timestamp']
        conflictTarget = ", ".join(conflictColumns)
        
        mergeQuery = f"""
        INSERT INTO {tableName} ({columnsToInsert})
        VALUES ({valuePlaceholders})
        ON CONFLICT ({conflictTarget}) 
        DO NOTHING;
        """
    elif tableName == 'stockquote':
        conflictColumns = ['symbol']
        conflictTarget = ", ".join(conflictColumns)
        updateSet = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns])
        
        mergeQuery = f"""
        INSERT INTO {tableName} ({columnsToInsert})
        VALUES ({valuePlaceholders})
        ON CONFLICT ({conflictTarget}) 
        DO UPDATE SET {updateSet};
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

    if not timestamps or not quote:
        print(f"No valid quote data for {ticker}.")
        return None

    # default to last ingex, iterate backward through 'close' values to find the first valid index
    validIndex = len(timestamps) - 1
    for i in range(validIndex, -1, -1):  # <- Start from the last index and move backwards
        if quote['close'][i] is not None:
            validIndex = i
            break  # <- Exit the loop once we find the first valid entry
        
    data = [{
        'timestamp': timestamps[validIndex],
        'high': quote['high'][validIndex],
        'low': quote['low'][validIndex],
        'open': quote['open'][validIndex],
        'close': quote['close'][validIndex],
        'volume': quote['volume'][validIndex],
    }]

    df = pd.DataFrame(data)
    df["symbol"] = ticker
    df["clubid"] = clubIdMapping.get(ticker, None)
    return df[['timestamp', 'high', 'low', 'open', 'close', 'volume', 'symbol', 'clubid']]


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
    return df[['symbol', 'timestamp', 'regularMarketPrice', 'marketCap', 'currency', 'exchangeTimezoneShortName', 'fullExchangeName', 'gmtOffSetMilliseconds', 'sharesOutstanding', 'beta', 'longName', 'clubid']]

# -----------------------------------------------------------------  #  
# ---------- Wrappers for fetch data and upload to DB -------------- #
# -----------------------------------------------------------------  #  
def fecthQuotesAndUploadToDB(tickers, session):
        quotes = getStockQuotes(tickers, session)
        quoteTableName = 'stockquote'
        quotesMQ = generateMergeQuery(quotes, quoteTableName)
        uploadToDB(quotes, quoteTableName, quotesMQ)

def fetchChartsAndUploadToDB(session, range):
        charts = pd.DataFrame()
        for ticker in tickers: 
            chart = getStockChartData(ticker, range, "1d", session)
            charts = pd.concat([chart, charts], ignore_index=True)
            chartTableName = 'stockchart'
            chartsMQ = generateMergeQuery(charts, chartTableName)
            uploadToDB(charts, chartTableName, chartsMQ)   

# -----------------------------------------------------------------  #  
# -------------- Checks if tables exists - else creates ------------ #
# -----------------------------------------------------------------  #  
def createTablesIfNotExists():
    for table in tables:
        if table == 'stockchart':
            run_sql_query("""
                CREATE TABLE IF NOT EXISTS stockchart (
                    timestamp BIGINT UNIQUE,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    open DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume INTEGER,
                    symbol VARCHAR(10),
                    clubid INT
                );
            """, commit_changes=True)
        if table == 'stockquote':
            run_sql_query("""
                CREATE TABLE IF NOT EXISTS stockquote (
                    symbol VARCHAR(10) UNIQUE,
                    clubid INT,
                    marketcap BIGINT,
                    currency VARCHAR(10),
                    exchangeTimezoneShortName VARCHAR(10),
                    fullExchangeName TEXT,
                    gmtOffSetMilliseconds INT,
                    sharesOutstanding BIGINT,
                    beta DOUBLE PRECISION,
                    longName TEXT,
                    regularMarketPrice DOUBLE PRECISION,
                    timestamp BIGINT
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

        createTablesIfNotExists()

        # TODO: Om table empty - hämta längre range, ny metod  / "köra migrering" en gång?
        # Run daily
        fetchChartsAndUploadToDB(session, "5d")

        # Run monthly
        currentQuoteData = run_sql_query("SELECT * FROM stockquote;")
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

integrationYahooFinance()
