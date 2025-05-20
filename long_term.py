# long_term.py
"""
Fetches 10 years of hourly adjusted OHLVC + Volume for a list of US stocks.
Saves hourly data per symbol and an aggregated daily volume CSV.

WARNING: This script can take a VERY LONG TIME to run for many symbols
         due to API request limits and pacing requirements.
         Run it in a stable environment where it can be left unattended.
         Start with a small list of symbols to test.
"""

import pandas as pd
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import os
import threading # <--- MISSING IMPORT ADDED HERE

# Import the IBDataApp class from our IBKR interaction module
# We will use its methods directly for more control over a long session.
from ib_functions import IBDataApp # Assuming IBDataApp is in ib_functions.py
from ibapi.contract import Contract # For creating contract objects

# --- Configuration ---
# LIST OF US STOCK SYMBOLS TO FETCH DATA FOR
# !!! START WITH A VERY SMALL LIST (2-3 symbols) FOR INITIAL TESTING !!!
# Once confirmed working, you can expand this list.
# Fetching for "thousands" will take days/weeks.
SYMBOLS_TO_FETCH: List[str] = [
    "AAPL", "MSFT", "GOOGL",
    # "AMZN", "NVDA", "TSLA", "BRK-B", "JPM", "JNJ", "V",
    # "PG", "UNH", "HD", "MA", "PYPL", "DIS", "NFLX", "ADBE", "CRM", "XOM",
    # "SPY", "QQQ" # Also works for ETFs if sec_type is adjusted
]

# Parameters for historical data request
YEARS_OF_DATA: int = 10
HOURLY_BAR_SIZE: str = "1 hour"
# For adjusted data (Open, High, Low, Close, Volume, WAP are adjusted)
HOURLY_WHAT_TO_SHOW: str = "ADJUSTED_LAST"

# How much data to request in each chunk (e.g., "1 M", "2 M")
# Smaller chunks are safer for API limits but mean more requests.
# IBKR limit for 1-hour bars is often around 1-2 months. Let's use 1 month.
CHUNK_DURATION: str = "1 M"

PRIMARY_EXCHANGE_MAP: Dict[str, str] = {
    "AAPL": "NASDAQ", "MSFT": "NASDAQ", "GOOGL": "NASDAQ", "AMZN": "NASDAQ",
    "NVDA": "NASDAQ", "TSLA": "NASDAQ", "NFLX": "NASDAQ", "ADBE": "NASDAQ",
    "CRM": "NYSE", "BRK-B": "NYSE", "JPM": "NYSE", "JNJ": "NYSE", "V": "NYSE",
    "PG": "NYSE", "UNH": "NYSE", "HD": "NYSE", "MA": "NYSE",
    "PYPL": "NASDAQ", "DIS": "NYSE", "XOM": "NYSE",
    "SPY": "ARCA", "QQQ": "NASDAQ"
}
DEFAULT_EXCHANGE: str = "SMART"
DEFAULT_CURRENCY: str = "USD"

# TWS Connection Parameters
TWS_PORT: int = 7497  # Default for TWS Paper trading
APP_CLIENT_ID: int = 1001 # A single client ID for the entire session

# Output directories and file names
OUTPUT_DIR_HOURLY: str = "hourly_data_10Y"
OUTPUT_DAILY_VOLUME_CSV: str = "all_daily_volumes_10Y.csv"

# Pacing between API requests for different symbols (in seconds)
SYMBOL_REQUEST_DELAY: int = 10 # Increase if facing pacing issues for many symbols
# Pacing between chunked requests for the SAME symbol
CHUNK_REQUEST_DELAY: int = 3 # Can be shorter, but still important

def create_output_dirs():
    """Creates output directories if they don't exist."""
    if not os.path.exists(OUTPUT_DIR_HOURLY):
        os.makedirs(OUTPUT_DIR_HOURLY)
        print(f"Created directory: {OUTPUT_DIR_HOURLY}")

def fetch_long_term_hourly_data_for_symbol(
    app: IBDataApp,
    symbol: str,
    sec_type: str,
    exchange: str,
    primary_exchange: Optional[str],
    currency: str,
    years_of_data: int,
    bar_size: str,
    what_to_show: str,
    chunk_duration: str
) -> Optional[pd.DataFrame]:
    """
    Fetches long-term hourly data for a single symbol by making chunked requests.
    """
    all_bars_for_symbol = []
    # Calculate number of months for chunking
    total_months = years_of_data * 12
    
    print(f"Preparing to fetch {total_months} monthly chunks for {symbol} ({years_of_data} years)...")

    # Create contract object once for the symbol
    base_contract = Contract()
    base_contract.symbol = symbol
    base_contract.secType = sec_type
    base_contract.currency = currency
    base_contract.exchange = exchange
    if primary_exchange:
        base_contract.primaryExchange = primary_exchange

    # Iterate backwards in time, fetching month by month
    # endDateTime for reqHistoricalData: "YYYYMMDD HH:MM:SS [TMZ]"
    # If TMZ is not specified, TWS local time is assumed.
    # It's often safer to specify UTC for intraday, but let's try TWS local first.
    end_datetime_marker = datetime.now()

    for i in range(total_months):
        # Format endDateTime for the API
        end_date_str_for_api = end_datetime_marker.strftime("%Y%m%d %H:%M:%S")
        
        print(f"  Fetching chunk {i+1}/{total_months} for {symbol}, ending around {end_date_str_for_api}")

        req_id = app.get_next_req_id()
        # print(f"    ReqId: {req_id}, EndDateTime for API: {end_date_str_for_api}") # Verbose
        
        completion_event = threading.Event()
        app._request_complete_events[req_id] = completion_event
        app.historical_data[req_id] = [] 
        app._error_message_for_request.pop(req_id, None)

        app.reqHistoricalData(
            reqId=req_id,
            contract=base_contract, # Use the same contract object
            endDateTime=end_date_str_for_api,
            durationStr=chunk_duration, # e.g., "1 M"
            barSizeSetting=bar_size,
            whatToShow=what_to_show,
            useRTH=1, # For stocks, usually RTH is what's wanted for hourly
            formatDate=1, # Request YYYYMMDD HH:MM:SS
            keepUpToDate=False,
            chartOptions=[]
        )
        
        timeout_chunk = 75 
        completed = completion_event.wait(timeout=timeout_chunk)

        if req_id in app._request_complete_events: del app._request_complete_events[req_id]
        
        chunk_bars = app.historical_data.pop(req_id, [])
        chunk_error = app._error_message_for_request.pop(req_id, None)

        if chunk_error:
            print(f"    Error fetching chunk for {symbol}: {chunk_error}")
            time.sleep(CHUNK_REQUEST_DELAY) 
            # Decrement end_datetime_marker to try fetching the period before this failed one
            end_datetime_marker -= timedelta(days=28) # Approximate for "1 M"
            continue 
        if not completed and not chunk_bars:
            print(f"    Timeout fetching chunk for {symbol}.")
            app.cancelHistoricalData(req_id)
            time.sleep(CHUNK_REQUEST_DELAY)
            end_datetime_marker -= timedelta(days=28)
            continue
        
        if chunk_bars:
            all_bars_for_symbol.extend(chunk_bars)
            print(f"    Fetched {len(chunk_bars)} bars for this chunk.")
            # Update end_datetime_marker based on the start of the received data if possible
            # This requires parsing the first bar's date.
            # For simplicity, we'll just step back by the chunk duration.
            if chunk_duration == "1 M":
                 end_datetime_marker -= timedelta(days=28) # Approx. one month
            # Add more logic here if using different CHUNK_DURATION values
        else:
            print(f"    No bars returned for this chunk of {symbol}. Moving to next period.")
            # Still move back the end_datetime_marker to avoid getting stuck
            if chunk_duration == "1 M":
                 end_datetime_marker -= timedelta(days=28)


        time.sleep(CHUNK_REQUEST_DELAY)

    if not all_bars_for_symbol:
        print(f"No bars collected for {symbol} after all chunk attempts.")
        return None

    df = pd.DataFrame([vars(bar) for bar in all_bars_for_symbol])
    if df.empty: return None

    try:
        date_col_series = df['date']
        if df['date'].iloc[0].isdigit() and len(df['date'].iloc[0]) == 8: # YYYYMMDD
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        else: # Assuming YYYYMMDD HH:MM:SS or similar
            normalized_dates = date_col_series.astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
            df['date'] = pd.to_datetime(normalized_dates, errors='coerce') # More robust parsing
        
        if df['date'].isnull().any():
             print(f"[Warning] Some dates resulted in NaT after parsing for {symbol}. Original head: {date_col_series.head().tolist()}")

        df.rename(columns={'date': 'DateTime', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume', 'barCount': 'BarCount', 'average': 'WAP'}, inplace=True)
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0).astype('int64')
        df.set_index('DateTime', inplace=True)
        
        df = df[~df.index.duplicated(keep='first')]
        df.sort_index(inplace=True)

        columns_to_keep = ['Open', 'High', 'Low', 'Close', 'Volume', 'WAP', 'BarCount']
        available_columns = [col for col in columns_to_keep if col in df.columns]
        df = df[available_columns]
    except Exception as e:
        print(f"Error during DataFrame formatting for {symbol}: {e}")
        return None 

    return df


def main():
    """Main function to orchestrate data fetching and aggregation."""
    create_output_dirs()
    app = IBDataApp() 

    print(f"Attempting to connect to TWS/Gateway on 127.0.0.1:{TWS_PORT} with Client ID {APP_CLIENT_ID}...")
    app.connect("127.0.0.1", TWS_PORT, clientId=APP_CLIENT_ID)

    # Global api_thread variable for the main connection
    global api_thread # Declare api_thread as global if it's defined outside and used in finally
    api_thread = threading.Thread(target=app.run_loop, name=f"IB_API_Main_Thread", daemon=True)
    api_thread.start()

    connection_timeout = 20
    print(f"Waiting up to {connection_timeout}s for connection confirmation...")
    connected = app._connection_event.wait(timeout=connection_timeout)

    if not connected or app._error_event.is_set() or app.next_valid_order_id is None:
        error_msg = app._general_error_message or "Connection timed out or failed before nextValidId received."
        print(f"CRITICAL: Failed to connect to IBKR: {error_msg}. Exiting.")
        if app.isConnected(): app.disconnect()
        return

    print("Connection to TWS successful. Starting data fetch process.")
    
    all_daily_volumes_dict = {} 

    try: # Wrap the main loop in try-finally to ensure disconnect
        for i, symbol in enumerate(SYMBOLS_TO_FETCH):
            print(f"\n--- Processing Symbol: {symbol} ({i+1}/{len(SYMBOLS_TO_FETCH)}) ---")
            
            primary_exchange = PRIMARY_EXCHANGE_MAP.get(symbol)
            sec_type = "STK" # Assuming ETFs are handled as STK by IBKR for historical data

            hourly_df = fetch_long_term_hourly_data_for_symbol(
                app=app,
                symbol=symbol,
                sec_type=sec_type,
                exchange=DEFAULT_EXCHANGE,
                primary_exchange=primary_exchange,
                currency=DEFAULT_CURRENCY,
                years_of_data=YEARS_OF_DATA,
                bar_size=HOURLY_BAR_SIZE,
                what_to_show=HOURLY_WHAT_TO_SHOW,
                chunk_duration=CHUNK_DURATION
            )

            if hourly_df is not None and not hourly_df.empty:
                hourly_csv_path = os.path.join(OUTPUT_DIR_HOURLY, f"{symbol}_hourly_{YEARS_OF_DATA}Y.csv")
                try:
                    hourly_df.to_csv(hourly_csv_path)
                    print(f"Successfully saved hourly data for {symbol} to {hourly_csv_path}")
                except Exception as e:
                    print(f"Error saving hourly CSV for {symbol}: {e}")

                if 'Volume' in hourly_df.columns:
                    daily_volume = hourly_df['Volume'].resample('D').sum()
                    daily_volume.name = symbol 
                    all_daily_volumes_dict[symbol] = daily_volume
                    print(f"Aggregated daily volume for {symbol}.")
                else:
                    print(f"Could not find 'Volume' column to aggregate daily volume for {symbol}.")
            else:
                print(f"No hourly data fetched for {symbol}. Skipping.")

            if i < len(SYMBOLS_TO_FETCH) - 1:
                print(f"Pausing for {SYMBOL_REQUEST_DELAY} seconds before next symbol...")
                time.sleep(SYMBOL_REQUEST_DELAY)

        if all_daily_volumes_dict:
            print("\nAggregating all daily volumes...")
            all_daily_volumes_df = pd.concat(all_daily_volumes_dict.values(), axis=1, keys=all_daily_volumes_dict.keys())
            all_daily_volumes_df.sort_index(inplace=True)
            try:
                all_daily_volumes_df.to_csv(OUTPUT_DAILY_VOLUME_CSV)
                print(f"Successfully saved aggregated daily volumes to {OUTPUT_DAILY_VOLUME_CSV}")
                print(all_daily_volumes_df.info())
            except Exception as e:
                print(f"Error saving daily volumes CSV: {e}")
        else:
            print("No daily volumes were aggregated.")

    finally: # Ensure disconnection happens
        if app.isConnected():
            print("\nDisconnecting from TWS/Gateway...")
            app.disconnect()
            time.sleep(2) 
            if api_thread.is_alive(): # Check if thread is defined and alive
                 print("[Warning] Main API thread still alive after disconnect.")
        
        print("\n--- Long-Term Data Fetch Script Finished ---")

if __name__ == "__main__":
    main()
