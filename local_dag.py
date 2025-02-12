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

# TICKERS FOR TESTING:
tickers = ["AIK-B.ST", "MANU"]

# Mapping for club IDs based on the symbol
clubIdMapping = {
    "AIK-B.ST": 1,
    "MANU": 2 
}

def uploadToDB(df: pd.DataFrame, table_name: str, merge_query: str):
    if df.empty:
        print(f"{datetime.now()}: Response recieved for {table_name} was empty. Skipping upload...")
        return

    merge_to_postgres(df, merge_query, len(df.columns))

    print("{} Loaded Yahoo Finance data to {}".format(datetime.now(), table_name))

def queryToDB(query: str):
    df = run_sql_query(query)
    return df
    
            
# Makes an API request and handles errors with retries if necessary.
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

def generateMergeQuery(df: pd.DataFrame, table_name: str):
    # Get columns from the dataframe
    columns = df.columns.tolist()
    
    # Determine the conflict column and what to update in case of a conflict
    if table_name == 'stockchart':
        conflict_columns = ['timestamp']
    elif table_name == 'stockquotes':
        conflict_columns = ['symbol']
    else:
        raise ValueError(f"Unknown table: {table_name}")

    value_placeholders = ", ".join(["%s"] * len(columns))
    columns_to_insert = ", ".join(columns)
    conflict_target = ", ".join(conflict_columns)
    
    # Create the SQL merge query
    merge_query = f"""
    INSERT INTO {table_name} ({columns_to_insert})
    VALUES ({value_placeholders})
    ON CONFLICT ({conflict_target}) 
    DO NOTHING;
    """
    return merge_query

# Get stock data for chosen ticker within range and interval
def getStockChart(ticker, withinRange, interval, session):
    url = f"{BASE_URL}/stock/get-chart?symbol={ticker}&lang=en-US&useYfid=true&includeAdjustedClose=true&events=div%2Csplit%2Cearn&range={withinRange}&interval={interval}"
    res = Request(url, session=session)

    if res is None or res.empty or 'chart' not in res or 'result' not in res['chart']:
        print(f"No valid data found for {ticker}.")
        return None

    chart_data = res['chart']['result'][0]
    timestamps = chart_data.get('timestamp', [])
    quote = chart_data.get('indicators', {}).get('quote', [])[0]

    if not timestamps or not quote:
        print(f"No valid quote data for {ticker}.")
        return None

    valid_index = len(timestamps) - 1  # default to last index

    # Iterate backward through 'close' values to find the first valid index
    for i in range(valid_index, -1, -1):  # <- Start from the last index and move backwards
        if quote['close'][i] is not None:
            valid_index = i
            break  # <- Exit the loop once we find the first valid entry
        
    data = [{
        'timestamp': timestamps[valid_index],
        'high': quote['high'][valid_index],
        'low': quote['low'][valid_index],
        'open': quote['open'][valid_index],
        'close': quote['close'][valid_index],
        'volume': quote['volume'][valid_index],
    }]

    # Create DataFrame and add columns
    df = pd.DataFrame(data)
    df["symbol"] = ticker
    df["clubid"] = clubIdMapping.get(ticker, None)
    return df[['timestamp', 'high', 'low', 'open', 'close', 'volume', 'symbol', 'clubid']]

# Get stock quotes, general info, can take max 200 tickers
def getStockQuotes(tickersToGet, session):
    ticker_query = "%2C".join(tickersToGet)
    url = f"{BASE_URL}/market/get-quotes?region=US&symbols={ticker_query}"
    res = Request(url, session=session)

    if res is None or res.empty or 'quoteResponse' not in res or 'result' not in res.get('quoteResponse', {}):
        print("Monthly stock quotes could not be found.")
        return None


    result_data = res['quoteResponse']['result']
    if not result_data:
        print("No result data found for monthly quotes.")
        return None

    # Create DataFrame and add columns      
    df = pd.DataFrame(result_data)
    df["clubid"] = df["symbol"].map(clubIdMapping)
    df["timestamp"] = int(datetime.now().timestamp())
    return df[['symbol', 'timestamp', 'regularMarketPrice', 'marketCap', 'currency', 'exchangeTimezoneShortName', 'fullExchangeName', 'gmtOffSetMilliseconds', 'sharesOutstanding', 'beta', 'longName', 'clubid']]

# Fetches data from Yahoo Finance Real Time
def integrationYahooFinance(): 
    with Session() as session:
        session.headers.update({
            "x-rapidapi-key": YAHOO_FINANCE_API_KEY,
            "x-rapidapi-host": YAHOO_FINANCE_HOST
        })

        # TODO: First check - has tables? 

        hasQuoteData = queryToDB("SELECT * FROM stockquotes;")
        if hasQuoteData.empty:
            print("No data in stockquotes, fetching...")
            quotes = getStockQuotes(tickers, session)
            quoteTableName = 'stockquotes'
            quotesMQ = generateMergeQuery(quotes, quoteTableName)
            uploadToDB(quotes, quoteTableName, quotesMQ)
        else:
            hasQuoteData['timestamp'] = pd.to_datetime(hasQuoteData['timestamp'], unit="s")
            latest_timestamp = hasQuoteData["timestamp"].max()
            one_month_ago = datetime.now() - timedelta(days=30)
            if latest_timestamp < one_month_ago:
                print("stockquotes data older than a month, fetching...")
                quotes = getStockQuotes(tickers, session)
                quoteTableName = 'stockquotes'
                quotesMQ = generateMergeQuery(quotes, quoteTableName)
                uploadToDB(quotes, quoteTableName, quotesMQ)
            else:
                print("stockquotes data is fetched within this month...")

        charts = pd.DataFrame()
        for ticker in tickers: 
            chart = getStockChart(ticker, "5d", "1d", session)
            charts = pd.concat([chart, charts], ignore_index=True)
            chartTableName = 'stockchart'
            chartsMQ = generateMergeQuery(charts, chartTableName)
            uploadToDB(charts, chartTableName, chartsMQ)

integrationYahooFinance()
