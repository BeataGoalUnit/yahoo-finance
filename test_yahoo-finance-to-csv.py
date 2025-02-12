from datetime import datetime
from requests import Session
import pandas as pd
import time
import sys

YAHOO_FINANCE_API_KEY = "137f78e0f7msh62e445a737cf689p109e79jsne0788c058c05"
YAHOO_FINANCE_HOST = "yahoo-finance-real-time1.p.rapidapi.com"
BASE_URL = "https://yahoo-finance-real-time1.p.rapidapi.com/"

# Tickers to fetch
tickers = ["AIK-B.ST", "MANU"]

# Mapping for club IDs based on the symbol
club_id_mapping = {
    "AIK-B.ST": 1,  # AIK has club_id 1
    "MANU": 2        # Manchester United has club_id 2
}

# Function to make API requests with error handling
def Request(url: str, session, attempt: int = 0):
    response = session.get(url)

    if response.status_code == 200:
        res = response.json()
        return res  # Returning raw JSON instead of DataFrame

    attemptCount = attempt + 1

    # Handling specific errors
    if response.status_code in [400, 404]:
        print(f"{response.status_code} {response.reason}. Skipping.")
        return None

    if response.status_code == 429: 
        print("429: Too many requests, retrying...")
        time.sleep(1)
        return Request(url=url, session=session)

    if response.status_code == 500:
        if attemptCount >= 10:
            print("10 attempts made, moving on.")
            return None
        print(f"500: Internal Server Error, attempt [{attemptCount}], retrying in 10 minutes...")
        time.sleep(600)
        return Request(url=url, session=session, attempt=attemptCount)

    if response.status_code == 503:
        if attemptCount >= 10:
            print("Aborting")
            sys.exit(1)
        print(f"503: Service unavailable, attempt [{attemptCount}], retrying in 2 minutes...")
        time.sleep(120)
        return Request(url=url, session=session, attempt=attemptCount)

    if response.status_code == 504:
        print(f"504: Gateway time-out, attempt [{attemptCount}], retrying in 30 seconds...")
        time.sleep(30)
        return Request(url=url, attempt=attemptCount)

    print(f"Unexpected error {response.status_code}: {response.reason}. More info: {response.text}")
    return None

def generateMergeQuery(table_name: str, df: pd.DataFrame):
    # Get columns from the dataframe
    columns = df.columns.tolist()
    
    # Determine the conflict column and what to update in case of a conflict
    if table_name == 'stockChart':
        conflict_columns = ['symbol', 'timestamp']
    elif table_name == 'stockQuotes':
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

    if not res or 'chart' not in res or 'result' not in res['chart']:
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
    for i in range(valid_index, -1, -1):  # Start from the last index and move backwards
        if quote['close'][i] is not None:
            valid_index = i
            break  # Exit the loop once we find the first valid entry
        
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
    df["club_id"] = club_id_mapping.get(ticker, None)

    return df[['timestamp', 'high', 'low', 'open', 'close', 'volume', 'symbol', 'club_id']]

# Refactor getQuotesMonthly to simplify
def getStockQuotes(tickersToGet, session):
    ticker_query = "%2C".join(tickersToGet)
    url = f"{BASE_URL}/market/get-quotes?region=US&symbols={ticker_query}"
    res = Request(url, session=session)

    if not res or 'quoteResponse' not in res or 'result' not in res['quoteResponse']:
        print("Monthly stock quotes could not be found.")
        return None

    result_data = res['quoteResponse']['result']
    if not result_data:
        print("No result data found for monthly quotes.")
        return None

    df = pd.DataFrame(result_data)
    df["club_id"] = df["symbol"].map(club_id_mapping)

    return df[['symbol', 'regularMarketPrice', 'marketCap', 'currency', 'exchangeTimezoneShortName', 'fullExchangeName', 'gmtOffSetMilliseconds', 'sharesOutstanding', 'beta', 'longName', 'club_id']]

# Helper function to save data as CSV
def saveToCSV(dataframe, filename):
    if dataframe is not None and not dataframe.empty:
        dataframe.to_csv(filename, index=False)
        print(f"Saved data to {filename}")
    else:
        print(f"No valid data to save for {filename}")

# Main function to fetch data and save to CSV
def fetchYahooData():
    with Session() as session:
        session.headers.update({
            "x-rapidapi-key": YAHOO_FINANCE_API_KEY,
            "x-rapidapi-host": YAHOO_FINANCE_HOST
        })

        today = datetime.now().strftime("%Y-%m-%d")

        # Fetch daily stock data - define df before iteration
        # concat per club
        charts = pd.DataFrame()
        for ticker in tickers:
            chart = getStockChart(ticker, "5d", "1d", session)
            if chart is not None:
                charts = pd.concat([charts, chart], ignore_index=True)

        chartMQ = generateMergeQuery('stockChart', charts)
        print(chartMQ)
        saveToCSV(charts, f"daily_stock_data_{today}.csv")

        # Fetch monthly quotes (run only on the 7th day of the month)
        if datetime.now().day == 7:
            quotes = getStockQuotes(tickers, session)
            quoteMQ = generateMergeQuery('stockQuotes', quotes)
            print(quoteMQ)
            saveToCSV(quotes, f"monthly_stock_quotes_{today}.csv")

# Run the function
fetchYahooData()
