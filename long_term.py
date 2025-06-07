# long_term.py
"""
Fetches 10 years of hourly UNADJUSTED (TRADES) OHLVC + Volume for US stocks.
Saves hourly data per symbol and an aggregated daily volume CSV.
Addresses date parsing issues and TWS timezone warnings.
"""

import pandas as pd
import time
from datetime import datetime, timedelta, timezone # Ensure timezone is imported
from typing import List, Dict, Optional
import os
import threading

from ib_functions import IBDataApp
from ibapi.contract import Contract

# --- Configuration ---
SYMBOLS_TO_FETCH: List[str] = [
    "AAPL", "MSFT", # "GOOGL", # Start with 1-2 symbols
]

YEARS_OF_DATA: int = 10
HOURLY_BAR_SIZE: str = "1 hour"
HOURLY_WHAT_TO_SHOW: str = "TRADES" # Must be TRADES for historical endDateTime chunking
CHUNK_DURATION: str = "2 M"

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

TWS_PORT: int = 7497
APP_CLIENT_ID: int = 1001

OUTPUT_DIR_HOURLY: str = "hourly_data_10Y_TRADES"
OUTPUT_DAILY_VOLUME_CSV: str = "all_daily_volumes_10Y_TRADES.csv"

SYMBOL_REQUEST_DELAY: int = 10
CHUNK_REQUEST_DELAY: int = 4
CHUNK_TIMEOUT_SECONDS: int = 90 # Increased from previous version

def create_output_dirs():
    if not os.path.exists(OUTPUT_DIR_HOURLY):
        os.makedirs(OUTPUT_DIR_HOURLY)
        print(f"Created directory: {OUTPUT_DIR_HOURLY}")

def parse_ibkr_datetime_str(date_str: str) -> Optional[datetime]:
    """
    Parses date strings from IBKR which might include a timezone abbreviation.
    Example: "20250324 09:30:00 US/Eastern" or "20250324"
    Returns a naive datetime object.
    """
    cleaned_date_str = date_str.strip()
    parts = cleaned_date_str.split(" ")
    
    datetime_part_to_parse = ""
    if len(parts) >= 1: # Date part
        datetime_part_to_parse = parts[0]
    if len(parts) >= 2 and ":" in parts[1]: # Time part
        datetime_part_to_parse += " " + parts[1]
    
    try:
        if len(datetime_part_to_parse) == 8: # YYYYMMDD
            return datetime.strptime(datetime_part_to_parse, "%Y%m%d")
        elif len(datetime_part_to_parse) >= 17: # YYYYMMDD HH:MM:SS
             # Ensure only one space between date and time if multiple were present
            datetime_part_to_parse = datetime_part_to_parse.replace("  ", " ")
            return datetime.strptime(datetime_part_to_parse, "%Y%m%d %H:%M:%S")
        else:
            print(f"[Date Parse Warning] Unrecognized date string format: '{date_str}' -> '{datetime_part_to_parse}'")
            return None
    except ValueError as ve:
        print(f"[Date Parse Error] Could not parse '{datetime_part_to_parse}' from original '{date_str}': {ve}")
        return None


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
    chunk_duration_str: str
) -> Optional[pd.DataFrame]:
    all_bars_for_symbol = []
    base_contract = Contract()
    base_contract.symbol = symbol
    base_contract.secType = sec_type
    base_contract.currency = currency
    base_contract.exchange = exchange
    if primary_exchange:
        base_contract.primaryExchange = primary_exchange

    # Work with UTC internally for target dates to avoid timezone confusion
    now_utc = datetime.now(timezone.utc)
    target_start_date_utc = now_utc - timedelta(days=years_of_data * 365.25)
    
    # end_datetime_marker for API requests will be in UTC
    end_datetime_marker_utc = now_utc

    print(f"Fetching data for {symbol} back to approximately {target_start_date_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}...")
    chunk_count = 0

    while end_datetime_marker_utc > target_start_date_utc:
        chunk_count += 1
        # Format endDateTime for the API in UTC: "YYYYMMDD HH:MM:SS UTC"
        end_date_str_for_api = end_datetime_marker_utc.strftime("%Y%m%d %H:%M:%S UTC")
        
        print(f"  Fetching chunk {chunk_count} for {symbol}, ending around {end_date_str_for_api} (duration {chunk_duration_str})")

        req_id = app.get_next_req_id()
        completion_event = threading.Event()
        app._request_complete_events[req_id] = completion_event
        app.historical_data[req_id] = [] 
        app._error_message_for_request.pop(req_id, None)

        app.reqHistoricalData(
            reqId=req_id, contract=base_contract, endDateTime=end_date_str_for_api,
            durationStr=chunk_duration_str, barSizeSetting=bar_size,
            whatToShow=what_to_show, useRTH=1, formatDate=1, # formatDate=1 still returns string
            keepUpToDate=False, chartOptions=[]
        )
        
        completed = completion_event.wait(timeout=CHUNK_TIMEOUT_SECONDS)
        if req_id in app._request_complete_events: del app._request_complete_events[req_id]
        
        chunk_bars = app.historical_data.pop(req_id, [])
        chunk_error = app._error_message_for_request.pop(req_id, None)

        approx_chunk_timedelta = timedelta(days=28 * int(chunk_duration_str.split(" ")[0])) if "M" in chunk_duration_str else timedelta(days=int(chunk_duration_str.split(" ")[0])) if "D" in chunk_duration_str else timedelta(days=30)

        if chunk_error:
            print(f"    Error fetching chunk for {symbol}: {chunk_error}")
            time.sleep(CHUNK_REQUEST_DELAY + 5) 
            end_datetime_marker_utc -= approx_chunk_timedelta
            continue 
        
        if not completed and not chunk_bars:
            print(f"    Timeout fetching chunk for {symbol}.")
            app.cancelHistoricalData(req_id) # Attempt to cancel
            time.sleep(CHUNK_REQUEST_DELAY + 5)
            end_datetime_marker_utc -= approx_chunk_timedelta
            continue
        
        if chunk_bars:
            all_bars_for_symbol.extend(chunk_bars)
            print(f"    Fetched {len(chunk_bars)} bars for this chunk.")
            first_bar_date_str = chunk_bars[0].date # e.g., "20250324 09:30:00 US/Eastern"
            parsed_first_bar_dt = parse_ibkr_datetime_str(first_bar_date_str)
            
            if parsed_first_bar_dt:
                # Assume parsed_first_bar_dt is naive local time of the exchange.
                # To make it comparable with our UTC markers, we'd ideally know the exchange's TZ.
                # For stepping back, using it as naive is okay if end_datetime_marker_utc is also made naive for this step.
                # Or, if TWS returns data aligned to UTC for intraday when UTC is requested, this is simpler.
                # Given the API warning, using UTC for endDateTime in request is better.
                # The dates returned by IBKR (formatDate=1) are often in the exchange's local time.
                # For robust stepping, we should convert this local exchange time to UTC.
                # This is complex without knowing the exact exchange timezone for each symbol.
                # Simplification: Assume the parsed_first_bar_dt can be treated as a naive marker
                # and we adjust our UTC end_datetime_marker_utc by a fixed duration.
                # This means the next endDateTime will be set to the start of the received chunk.
                
                # For now, let's assume the first bar's datetime can be used to set the next end_datetime_marker
                # We make it naive, then localize to UTC assuming it was exchange local time.
                # This is still tricky. A simpler robust step-back is by the chunk duration.
                end_datetime_marker_utc = end_datetime_marker_utc - approx_chunk_timedelta
                # More precise would be:
                # end_datetime_marker_utc = pytz.timezone("America/New_York").localize(parsed_first_bar_dt).astimezone(timezone.utc) - timedelta(seconds=1)
                # But this requires knowing the exchange timezone.
                # Let's stick to the approximate step back for now for simplicity,
                # and rely on deduplication later.
                print(f"    Next request for {symbol} will end before approx {parsed_first_bar_dt if parsed_first_bar_dt else 'failed_parse'}")

            else: # Parsing failed
                print(f"    Could not parse first bar date. Stepping back approximately.")
                end_datetime_marker_utc -= approx_chunk_timedelta
        else: 
            print(f"    No bars returned for this chunk of {symbol}. Stepping back period.")
            end_datetime_marker_utc -= approx_chunk_timedelta

        if end_datetime_marker_utc < target_start_date_utc - timedelta(days=30): # Buffer
             print(f"    End marker {end_datetime_marker_utc.strftime('%Y-%m-%d')} is before target {target_start_date_utc.strftime('%Y-%m-%d')}. Stopping for {symbol}.")
             break
        time.sleep(CHUNK_REQUEST_DELAY)

    if not all_bars_for_symbol:
        print(f"No bars collected for {symbol} after all chunk attempts.")
        return None

    df = pd.DataFrame([vars(bar) for bar in all_bars_for_symbol])
    if df.empty: return None

    try:
        parsed_dates = [parse_ibkr_datetime_str(date_str) for date_str in df['date'].astype(str)]
        df['DateTime'] = pd.Series(parsed_dates, index=df.index)
        
        if df['DateTime'].isnull().any():
             print(f"[Warning] Some DateTime values are NaT after parsing for {symbol}.")
        
        df.drop(columns=['date'], inplace=True, errors='ignore') # Drop original string date column
        df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume', 'barCount': 'BarCount', 'average': 'WAP'}, inplace=True)
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0).astype('int64')
        
        # Filter out rows where DateTime parsing failed (NaT) before setting index
        df.dropna(subset=['DateTime'], inplace=True)
        if df.empty:
            print(f"[Warning] DataFrame became empty after dropping NaT DateTimes for {symbol}.")
            return pd.DataFrame()
            
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
    create_output_dirs()
    app = IBDataApp() 

    print(f"Attempting to connect to TWS/Gateway on 127.0.0.1:{TWS_PORT} with Client ID {APP_CLIENT_ID}...")
    app.connect("127.0.0.1", TWS_PORT, clientId=APP_CLIENT_ID)

    global api_thread 
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
    print(f"Fetching UNADJUSTED data (whatToShow='{HOURLY_WHAT_TO_SHOW}'). Chunk duration: {CHUNK_DURATION}")
    
    all_daily_volumes_dict = {} 

    try: 
        for i, symbol in enumerate(SYMBOLS_TO_FETCH):
            print(f"\n--- Processing Symbol: {symbol} ({i+1}/{len(SYMBOLS_TO_FETCH)}) ---")
            
            primary_exchange = PRIMARY_EXCHANGE_MAP.get(symbol)
            sec_type = "STK" 

            hourly_df = fetch_long_term_hourly_data_for_symbol(
                app=app, symbol=symbol, sec_type=sec_type, exchange=DEFAULT_EXCHANGE,
                primary_exchange=primary_exchange, currency=DEFAULT_CURRENCY,
                years_of_data=YEARS_OF_DATA, bar_size=HOURLY_BAR_SIZE,
                what_to_show=HOURLY_WHAT_TO_SHOW, chunk_duration_str=CHUNK_DURATION
            )

            if hourly_df is not None and not hourly_df.empty:
                hourly_csv_path = os.path.join(OUTPUT_DIR_HOURLY, f"{symbol}_hourly_{YEARS_OF_DATA}Y.csv")
                try:
                    hourly_df.to_csv(hourly_csv_path)
                    print(f"Successfully saved hourly data for {symbol} to {hourly_csv_path}")
                except Exception as e:
                    print(f"Error saving hourly CSV for {symbol}: {e}")

                if 'Volume' in hourly_df.columns and not hourly_df.index.hasnans: # Check for NaNs in index
                    daily_volume = hourly_df['Volume'].resample('D').sum()
                    daily_volume.name = symbol 
                    all_daily_volumes_dict[symbol] = daily_volume
                    print(f"Aggregated daily volume for {symbol}.")
                elif hourly_df.index.hasnans:
                    print(f"Could not aggregate daily volume for {symbol} due to NaT in DateTimeIndex.")
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
            except Exception as e:
                print(f"Error saving daily volumes CSV: {e}")
        else:
            print("No daily volumes were aggregated.")

    finally: 
        if app.isConnected():
            print("\nDisconnecting from TWS/Gateway...")
            app.disconnect()
            time.sleep(2) 
            if 'api_thread' in locals() and api_thread.is_alive(): 
                 print("[Warning] Main API thread still alive after disconnect.")
        
        print("\n--- Long-Term Data Fetch Script Finished ---")

if __name__ == "__main__":
    main()
