from datetime import datetime
from requests import Session
import pandas as pd
from local_db_connection import merge_to_postgres
from local_db_connection import run_sql_query
import sys
import time
from datetime import datetime

YAHOO_FINANCE_API_KEY = "cbb9ea0430msh6efe1cb28bb8201p19401fjsn7a8d4adfc565" 
YAHOO_FINANCE_HOST = "yahoo-finance-real-time1.p.rapidapi.com"
BASE_URL = "https://yahoo-finance-real-time1.p.rapidapi.com/"

schemaName = 'financial'
tables = ['stock', 'dailystock', 'stockupdate', 'dailyindex', 'exchangetoeur']
indexes = ["^OMX", "^OMXC25", "^DJUS", "^FTSE", "^GDAXI", "^AEX", "FTSEMIB.MI", "PSI20.LS", "XU100.IS"]
tickers = ["AIK-B.ST", "MANU", "AAB.CO", "AGF-B.CO", "PARKEN.CO", "BIF.CO", "CCP.L", "BVB.DE", "AJAX.AS", "JUVE.MI", "SSL.MI", "FCP.LS", "SLBEN.LS", "SCP.LS", "FENER.IS", "GSRAY.IS", "BJKAS.IS", "TSPOR.IS"]

currencies = {
    "SEK": 133,
    "DKK": 45,
    "USD": 192,
    "GBP": 208,
    "TRY": 148,
    "EUR": 217
}

tickerIdMapping = {
    "AIK-B.ST": {
        "clubid": 9185,
        "stockid": 1
    },
    "MANU": {
        "clubid": 2188,
        "stockid": 2
    },
    "AAB.CO": {
        "clubid": 31088,
        "stockid": 3
    },
    "AGF-B.CO": {
        "clubid": 31974,
        "stockid": 4
    },
    "PARKEN.CO": {
        "clubid": 31402,
        "stockid": 5
    },
    "BIF.CO": {
        "clubid": 31169,
        "stockid": 6
    },
    "CCP.L": {
        "clubid": 671,
        "stockid": 7
    },
    "BVB.DE": {
        "clubid": 31914,
        "stockid": 8
    },
    "AJAX.AS": {
        "clubid": 15623,
        "stockid": 9
    },
    "JUVE.MI": {
        "clubid": 21376,
        "stockid": 10
    },
    "SSL.MI": {
        "clubid": 21439,
        "stockid": 11
    },
    "FCP.LS": {
        "clubid": 13421,
        "stockid": 12
    },
    "SLBEN.LS": {
        "clubid": 13031,
        "stockid": 13
    },
    "SCP.LS": {
        "clubid": 13547,
        "stockid": 14
    },
    "FENER.IS": {
        "clubid": 8082,
        "stockid": 15
    },
    "GSRAY.IS": {
        "clubid": 8094,
        "stockid": 16
    },
    "BJKAS.IS": {
        "clubid": 7951,
        "stockid": 17
    },
    "TSPOR.IS": {
        "clubid": 8444,
        "stockid": 18
    }
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

    if tableName == 'stock':
        print(tableName)
        conflictColumns = ['ticker']
        conflictTarget = ", ".join(conflictColumns)
        mergeQuery = f"""
        INSERT INTO {schemaName}.{tableName} ({columnsToInsert})
        VALUES ({valuePlaceholders})
        ON CONFLICT ({conflictTarget})
        DO NOTHING;
        """
    elif tableName == 'dailystock':
        conflictColumns = ['stockid', 'dateutc']
        conflictTarget = ", ".join(conflictColumns)
        mergeQuery = f"""
        INSERT INTO {schemaName}.{tableName} ({columnsToInsert})
        VALUES ({valuePlaceholders})
        ON CONFLICT ({conflictTarget}) 
        DO UPDATE SET 
            timestamp = EXCLUDED.timestamp,
            open = EXCLUDED.open,
            close = EXCLUDED.close,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            adjclose = EXCLUDED.adjclose,
            volume = EXCLUDED.volume;
        """
    elif tableName == 'dailyindex':
        conflictColumns = ['ticker', 'dateutc']
        conflictTarget = ", ".join(conflictColumns)
        mergeQuery = f"""
        INSERT INTO {schemaName}.{tableName} ({columnsToInsert})
        VALUES ({valuePlaceholders})
        ON CONFLICT ({conflictTarget}) 
        DO UPDATE SET 
            timestamp = EXCLUDED.timestamp,
            open = EXCLUDED.open,
            close = EXCLUDED.close,
            high = EXCLUDED.high,
            low = EXCLUDED.low;
        """
    elif tableName == 'stockupdate':
        conflictColumns = ['stockid', 'sharesoutstanding', 'beta', 'bookvalue' ]
        conflictTarget = ", ".join(conflictColumns)
        mergeQuery = f"""
        INSERT INTO {schemaName}.{tableName} ({columnsToInsert})
        VALUES ({valuePlaceholders})
        ON CONFLICT ({conflictTarget})
        DO NOTHING;
        """
    elif tableName == 'exchangetoeur':
        conflictColumns = ['fromcurrencyid', 'dateutc']
        conflictTarget = ", ".join(conflictColumns)
        mergeQuery = f"""
        INSERT INTO {schemaName}.{tableName} ({columnsToInsert})
        VALUES ({valuePlaceholders})
        ON CONFLICT ({conflictTarget}) 
        DO UPDATE SET 
            rate = EXCLUDED.rate,
            timestamp = EXCLUDED.timestamp;
        """
    else:
        raise ValueError(f"Unknown table: {tableName}")
    return mergeQuery
# -----------------------------------------------------------------  #  
# ---- Get stock data - general info - can take max 200 tickers ---- #
# -----------------------------------------------------------------  #  
def getStockData(session):
    tickerQuery = "%2C".join(tickers)
    url = f"{BASE_URL}/market/get-quotes?region=US&symbols={tickerQuery}"
    res = Request(url, session=session)

    if res is None or res.empty or 'quoteResponse' not in res or 'result' not in res.get('quoteResponse', {}):
        print("Stock quotes could not be found.")
        return None
    resultData = res['quoteResponse']['result']
    
    df = pd.DataFrame(resultData)
    df["clubid"] = df["symbol"].map(lambda x: tickerIdMapping.get(x, {}).get("clubid"))
    df["currencyid"] = df["currency"].str.upper().map(currencies)
    df["financialcurrencyid"] = df["financialCurrency"].str.upper().map(currencies)
    df["ticker"] = df["symbol"]

    return df[['ticker', 'shortName', 'currencyid', 'financialcurrencyid', 'exchangeTimezoneShortName', 'exchange', 'fullExchangeName', 'gmtOffSetMilliseconds', 'longName', 'clubid' ]]

# -----------------------------------------------------------------  #  
# --- Get stock data for chosen ticker within range and interval --- #
# -----------------------------------------------------------------  #  
def getDailyStockData(ticker, withinRange, interval, session):
    url = f"{BASE_URL}/stock/get-chart?symbol={ticker}&includeAdjustedClose=true&range={withinRange}&interval={interval}"
    res = Request(url, session=session)
    if res is None or res.empty or 'chart' not in res or 'result' not in res['chart']:
        print(f"No valid data found for {ticker}.")
        return None

    chartData = res['chart']['result'][0]
    timestamps = chartData.get('timestamp', [])
    quote = chartData.get('indicators', {}).get('quote', [])[0]
    adjclose = chartData.get('indicators', {}).get('adjclose', [])[0]

    valid_data = []
    for i in range(len(timestamps)):
        if quote['close'][i] is not None:
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
    df["stockid"] = tickerIdMapping.get(ticker, {}).get("stockid")
    df["dateutc"] = pd.to_datetime(df["timestamp"], unit='s', utc=True).dt.date
    return df[['timestamp', 'high', 'low', 'open', 'close', 'adjclose', 'volume', 'dateutc', 'stockid']]

# -----------------------------------------------------------------  #  
# ---- Get stock quotes, general info, can take max 200 tickers ---- #
# -----------------------------------------------------------------  #  
def getStockDataOnUpdate(session):
    tickerQuery = "%2C".join(tickers)
    url = f"{BASE_URL}/market/get-quotes?region=US&symbols={tickerQuery}"
    res = Request(url, session=session)
    if res is None or res.empty or 'quoteResponse' not in res or 'result' not in res.get('quoteResponse', {}):
        print("Stock data on update could not be found.")
        return None
    
    resultData = res.get('quoteResponse', {}).get('result')
    df = pd.DataFrame(resultData)
    df["stockid"] = df["symbol"].map(lambda x: tickerIdMapping.get(x, {}).get("stockid"))
    df["timestamp"] = int(datetime.now().timestamp())
    df["dateutc"] = pd.to_datetime(df["timestamp"], unit='s', utc=True).dt.date
    return df[['timestamp', 'sharesOutstanding', 'beta', 'bookValue', 'stockid', 'dateutc']]

# -----------------------------------------------------------------  #  
# --- Get index data for chosen ticker within range and interval --- #
# -----------------------------------------------------------------  # 
def getDailyIndexData(index, withinRange, interval, session):
    url = f"{BASE_URL}/stock/get-chart?symbol={index}&range={withinRange}&interval={interval}"
    res = Request(url, session=session)
    if res is None or res.empty or 'chart' not in res or 'result' not in res['chart']:
        print(f"No valid data found for {index}.")
        return None

    indexData = res['chart']['result'][0]
    timestamps = indexData.get('timestamp', [])
    meta = indexData.get('meta', {})
    quote = indexData.get('indicators', {}).get('quote', [])[0]

    valid_data = []
    for i in range(len(timestamps)):
        if quote['close'][i] is not None:
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
    df["ticker"] = index
    df["dateutc"] = pd.to_datetime(df["timestamp"], unit='s', utc=True).dt.date
    df["shortname"] = meta.get('shortName')
    df["currencyid"] = currencies.get(meta.get('currency'))
    return df[['timestamp', 'shortname', 'currencyid', 'high', 'low', 'open', 'close', 'dateutc', 'ticker']]

# -----------------------------------------------------------------  #  
# --- Get stock data for chosen ticker within range and interval --- #
# -----------------------------------------------------------------  #  
def getExchangeRatesToEurData(currency, withinRange, interval, session):
    url = f"{BASE_URL}/stock/get-chart?symbol={currency}EUR%3DX&range={withinRange}&interval={interval}"
    res = Request(url, session=session)
    if res is None or res.empty or 'chart' not in res or 'result' not in res['chart']:
        print(f"No valid data found for {currency}.")
        return None

    chartData = res['chart']['result'][0]
    timestamps = chartData.get('timestamp', [])
    quote = chartData.get('indicators', {}).get('quote', [])[0]

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
    df["fromcurrencyid"] = currencies.get(currency)
    df["dateutc"] = pd.to_datetime(df["timestamp"], unit='s', utc=True).dt.date
    return df[['timestamp', 'rate', 'dateutc', 'fromcurrencyid']]

# -----------------------------------------------------------------  #  
# ---------- Wrappers for fetch data and upload to DB -------------- #
# -----------------------------------------------------------------  #  
def fetchStockAndUploadToDB(session):
        stockData = getStockData(session)
        if stockData is None or stockData.empty:
            print('No stock data retrieved. Skipping upload...')
            return
        
        stockTableName = 'stock'
        stockMQ = generateMergeQuery(stockData, stockTableName)
        uploadToDB(stockData, stockTableName, stockMQ)

def fetchStockOnUpdateAndUploadToDB(session):
        onUpdateStock = getStockDataOnUpdate(session)
        if onUpdateStock is  None or onUpdateStock.empty:
            print('No stockupdate data retrieved. Skipping upload...')
            return
        
        onUpdateStockTableName = 'stockupdate'
        onUpdateStockMQ = generateMergeQuery(onUpdateStock, onUpdateStockTableName)
        uploadToDB(onUpdateStock, onUpdateStockTableName, onUpdateStockMQ)

def fetchDailyStockAndUploadToDB(session, range, interval):
    for ticker in tickers: 
        dailyStock = getDailyStockData(ticker, range, interval, session)
        if dailyStock is None or dailyStock.empty:
            print(f'No dailystock data retrieved for {ticker}. Skipping upload...')
            return
        
        dailyStockTableName = 'dailystock'
        dailyStockMQ = generateMergeQuery(dailyStock, dailyStockTableName)
        uploadToDB(dailyStock, dailyStockTableName, dailyStockMQ)   

def fetchDailyIndexAndUploadToDB(session, range, interval):
    for index in indexes:
        dailyIndex = getDailyIndexData(index, range, interval, session)
        if dailyIndex is None or dailyIndex.empty:
            print(f'No dailyindex data retrieved for {index}. Skipping upload...')
            return
        
        dailyIndexTableName = 'dailyindex'
        dailyIndexMQ = generateMergeQuery(dailyIndex, dailyIndexTableName)
        uploadToDB(dailyIndex, dailyIndexTableName, dailyIndexMQ)  

def fetchDailyExchangeRateAndUploadToDB(session, range, interval):
    for currency in currencies: 
        exchangeRate = getExchangeRatesToEurData(currency, range, interval, session)
        if exchangeRate is None or exchangeRate.empty:
            print(f'No exchangeRate data retrieved for {currency}. Skipping upload...')
            return
    
        exchangeRatesTableName = 'exchangetoeur'
        exchangeRatesMQ = generateMergeQuery(exchangeRate, exchangeRatesTableName)
        uploadToDB(exchangeRate, exchangeRatesTableName, exchangeRatesMQ)   

# -----------------------------------------------------------------  #  
# --- Checks if schema and / or tables exists - else creates ------- #
# -----------------------------------------------------------------  #  
def createSchemaIfNotExists():
    run_sql_query(f"CREATE SCHEMA IF NOT EXISTS {schemaName};", commit_changes=True)

def createTablesIfNotExists():
    for table in tables:
        if table == 'dailystock':
            run_sql_query(f"""
                CREATE TABLE IF NOT EXISTS {schemaName}.{table} (
                    dailystockid SERIAL PRIMARY KEY,
                    stockid INT,
                    timestamp BIGINT,
                    high NUMERIC(18,2),
                    low NUMERIC(18,2),
                    open NUMERIC(18,2),
                    close NUMERIC(18,2),
                    adjclose NUMERIC(18,2),
                    volume INTEGER,
                    dateutc DATE,
                    CONSTRAINT uc_dateutc_stockid UNIQUE (stockid, dateutc),
                    CONSTRAINT fk_stock_dailystock FOREIGN KEY (stockid) REFERENCES financial.stock(stockid)
                );
            """, commit_changes=True)
        if table == 'stockupdate':
            run_sql_query(f"""
                CREATE TABLE IF NOT EXISTS {schemaName}.{table} (
                    stockupdateid SERIAL PRIMARY KEY,
                    stockid INT,
                    sharesOutstanding BIGINT,
                    beta NUMERIC(18,2),
                    bookValue NUMERIC(18,2),
                    timestamp BIGINT, 
                    dateutc DATE,
                    CONSTRAINT uc_stockupdate UNIQUE (stockid, sharesOutstanding, beta, bookValue),
                    CONSTRAINT fk_stock_stockupdate FOREIGN KEY (stockid) REFERENCES financial.stock(stockid)
                );
            """, commit_changes=True)
        if table == 'stock':
            run_sql_query(f"""
                CREATE TABLE IF NOT EXISTS {schemaName}.{table} (
                    stockid SERIAL PRIMARY KEY,
                    ticker VARCHAR(10),
                    shortName TEXT,
                    clubid INT, 
                    currencyid INT,
                    financialcurrencyid INT,
                    exchangeTimezoneShortName VARCHAR(10),
                    exchange VARCHAR(10),
                    fullExchangeName TEXT,
                    gmtOffSetMilliseconds INT,
                    longName TEXT,
                    CONSTRAINT uc_ticker_stock UNIQUE (ticker),
                    CONSTRAINT fk_club_stock FOREIGN KEY (clubid) REFERENCES club.club(clubid),
                    CONSTRAINT fk_currency_stock FOREIGN KEY (currencyid) REFERENCES config.currency(currencyid),
                    CONSTRAINT fk_financialcurrencyid_stock FOREIGN KEY (financialcurrencyid) REFERENCES config.currency(currencyid)
                );
            """, commit_changes=True)
        if table == 'dailyindex':
            run_sql_query(f"""
                CREATE TABLE IF NOT EXISTS {schemaName}.{table} (
                    dailyindexid SERIAL PRIMARY KEY,
                    timestamp BIGINT,
                    currencyid INT,
                    shortName TEXT,
                    high NUMERIC(18,2),
                    low NUMERIC(18,2),
                    open NUMERIC(18,2),
                    close NUMERIC(18,2),
                    ticker VARCHAR(10),
                    dateutc DATE,
                    CONSTRAINT uc_ticker_dateutc UNIQUE (ticker, dateutc),
                    CONSTRAINT fk_currency_dailyindex FOREIGN KEY (currencyid) REFERENCES config.currency(currencyid)
                );
            """, commit_changes=True)
        if table == 'exchangetoeur':
            run_sql_query(f"""
                CREATE TABLE IF NOT EXISTS {schemaName}.{table} (
                    exchangetoeurid SERIAL PRIMARY KEY,
                    timestamp BIGINT,
                    rate NUMERIC(18,2),
                    fromcurrencyid INT,
                    dateutc DATE,
                    CONSTRAINT uc_dateutc_currency UNIQUE (fromcurrencyid, dateutc),
                    CONSTRAINT fk_currency_exchangetoeur FOREIGN KEY (fromcurrencyid) REFERENCES config.currency(currencyid)
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

        # Run only if table is empty
        currentStockData = run_sql_query(f"SELECT * FROM {schemaName}.stock;", commit_changes=False)
        if currentStockData.empty:
            print("No data in stock, fetching...")
            fetchStockAndUploadToDB(session)
        else:
            print("Has data in stock table...")

        # Run daily
        # TODO: Om hämta historisk daglig data, sätt withinRange till "10y"
        withinRange = "5d"
        fetchDailyStockAndUploadToDB(session, withinRange, "1d")
        fetchDailyIndexAndUploadToDB(session, withinRange, "1d")
        fetchDailyExchangeRateAndUploadToDB(session, withinRange, "1d")
        fetchStockOnUpdateAndUploadToDB(session)

# TODO: Create separate steps for compare index and exchange rate? Would create new sessions
integrationYahooFinance()
