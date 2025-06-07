# bulk_daily_data_fetcher.py
"""
Fetches long-term daily adjusted close prices for a user-defined list of US stocks
and aggregates them into a single CSV file.

WARNING: Fetching data for a very large list of symbols will take a significant
         amount of time due to API request limits and necessary pacing.
         Ensure TWS/Gateway is running and stable for the duration.
"""

import pandas as pd
import time
from typing import List, Dict, Optional, Tuple
import os

# Make sure ib_functions.py is in the same directory or your Python path
from ib_functions import get_historical_data

# --- Configuration ---

SYMBOLS_TO_FETCH: List[str] = []
PRIMARY_EXCHANGE_MAP: Dict[str, str] = {}
SYMBOLS_CSV_PATH = 'data/symbols_for_fetcher.csv' # Path to your CSV

YEARS_OF_DATA: int = 20
BAR_SIZE: str = "1 day"

DEFAULT_EXCHANGE: str = "SMART"
DEFAULT_CURRENCY: str = "USD"

TWS_PORT: int = 7497
STARTING_CLIENT_ID: int = 1201
SYMBOL_REQUEST_DELAY: int = 1 # Increased slightly for better stability

# --- NEW Output File Name ---
OUTPUT_CSV_FILE: str = f"aggregated_daily_adj_close_{YEARS_OF_DATA}Y_v2.csv"

# --- Load symbols and exchanges from the CSV file ---
try:
    print(f"Loading symbols from {SYMBOLS_CSV_PATH}...")
    # Read CSV, assuming it HAS a header row
    symbol_list_df = pd.read_csv(SYMBOLS_CSV_PATH)

    # *** IMPORTANT FIX: Clean the Ticker column FIRST ***
    if 'Ticker' in symbol_list_df.columns:
        symbol_list_df['Ticker'] = symbol_list_df['Ticker'].str.strip().str.upper()
    else:
        raise ValueError("'Ticker' column not found in CSV.")

    # Now use the cleaned 'Ticker' column
    SYMBOLS_TO_FETCH = symbol_list_df['Ticker'].dropna().unique().tolist()
    print(f"Loaded {len(SYMBOLS_TO_FETCH)} symbols.")

    if 'PrimaryExchange' in symbol_list_df.columns:
        # Create map ONLY for rows where PrimaryExchange is NOT NaN
        exchange_df = symbol_list_df.dropna(subset=['Ticker', 'PrimaryExchange'])
        # Use the already cleaned/uppercased 'Ticker' as the index
        PRIMARY_EXCHANGE_MAP = pd.Series(
            exchange_df.PrimaryExchange.values,
            index=exchange_df.Ticker
        ).to_dict()
        print(f"Loaded primary exchange map for {len(PRIMARY_EXCHANGE_MAP)} symbols.")
    else:
        print("Warning: 'PrimaryExchange' column not found. Using SMART for all.")

except FileNotFoundError:
    print(f"ERROR: File not found - {SYMBOLS_CSV_PATH}. Using fallback.")
    SYMBOLS_TO_FETCH = ["AAPL", "MSFT"] # Fallback
except Exception as e:
    print(f"ERROR loading {SYMBOLS_CSV_PATH}: {e}. Using fallback.")
    SYMBOLS_TO_FETCH = ["AAPL", "MSFT"] # Fallback

# --- Check if SYMBOLS_TO_FETCH is populated ---
if not SYMBOLS_TO_FETCH:
    print("CRITICAL ERROR: No symbols were loaded. Exiting.")
    exit()

def fetch_all_symbols_data(
    symbols: List[str],
    years_of_history: int,
    bar_size: str,
    port: int,
    start_client_id: int
) -> Tuple[Optional[pd.DataFrame], List[str]]:
    """
    Fetches historical daily adjusted close data for a list of symbols.
    Returns an aggregated DataFrame and a list of symbols that failed.
    """
    all_adj_closes_series: List[pd.Series] = []
    successful_symbols: List[str] = []
    failed_symbols: List[str] = []
    current_client_id = start_client_id
    duration_string = f"{years_of_history} Y"

    print(f"Starting bulk daily adjusted close data fetch for {len(symbols)} symbols...")
    print(f"Requesting {duration_string} of '{bar_size}' data.")
    print(f"Pacing delay between symbols: {SYMBOL_REQUEST_DELAY} seconds.")

    for i, symbol in enumerate(symbols):
        # We know 'symbol' from SYMBOLS_TO_FETCH is already uppercase
        print(f"\n({i+1}/{len(symbols)}) Fetching data for: {symbol}")

        # *** IMPORTANT FIX: Use 'symbol' directly as it's already uppercase ***
        primary_exchange = PRIMARY_EXCHANGE_MAP.get(symbol) # No .upper() needed here

        # --- Debug Print: See what exchange is being used ---
        print(f"  --> Using Primary Exchange: {primary_exchange if primary_exchange else 'SMART (Default)'}")
        # --- End Debug Print ---

        sec_type = "ETF" if symbol in ["SPY", "QQQ", "DIA", "IWM"] else "STK"

        daily_data_df = get_historical_data(
            symbol=symbol, # Already uppercase
            sec_type=sec_type,
            exchange=DEFAULT_EXCHANGE,
            primary_exchange=primary_exchange,
            currency=DEFAULT_CURRENCY,
            duration=duration_string,
            bar_size=bar_size,
            use_rth=True,
            port=port,
            client_id=current_client_id
        )
        current_client_id += 1 # Increment client ID for each request

        if daily_data_df is not None and not daily_data_df.empty:
            if 'Close' in daily_data_df.columns:
                series_renamed = daily_data_df['Close'].rename(symbol)
                all_adj_closes_series.append(series_renamed)
                successful_symbols.append(symbol)
                print(f"  Successfully fetched {len(series_renamed)} data points for {symbol}.")
            else:
                print(f"  Warning: 'Close' column not found for {symbol}.")
                failed_symbols.append(symbol + " (No Close Column)")
        else:
            print(f"  Failed to fetch data or no data returned for {symbol}.")
            failed_symbols.append(symbol + " (Fetch Fail/Empty)")

        if i < len(symbols) - 1: # Don't sleep after the last symbol
            print(f"  Pausing for {SYMBOL_REQUEST_DELAY} seconds...")
            time.sleep(SYMBOL_REQUEST_DELAY)

    if not all_adj_closes_series:
        print("No data fetched for any symbol.")
        return None, failed_symbols

    print("\nAggregating all fetched daily adjusted close data...")
    aggregated_df = pd.concat(all_adj_closes_series, axis=1, join='outer')
    # If keys weren't set correctly before, this ensures columns match symbols:
    aggregated_df.columns = successful_symbols
    aggregated_df.sort_index(inplace=True)

    print("Aggregation complete.")
    return aggregated_df, failed_symbols


if __name__ == "__main__":
    print("--- Bulk Daily Adjusted Close Price Fetcher ---")
    print(f"Output will be saved to: {OUTPUT_CSV_FILE}")
    print("WARNING: This can take a very long time for large symbol lists!")
    print("--- IMPORTANT: Ensure TWS or IB Gateway is running and API is enabled! ---")

    start_time = time.time()

    aggregated_data, failed_fetch_symbols = fetch_all_symbols_data(
        symbols=SYMBOLS_TO_FETCH,
        years_of_history=YEARS_OF_DATA,
        bar_size=BAR_SIZE,
        port=TWS_PORT,
        start_client_id=STARTING_CLIENT_ID
    )

    end_time = time.time()
    print(f"\nTotal fetching and aggregation time: {(end_time - start_time)/60:.2f} minutes.")

    if aggregated_data is not None and not aggregated_data.empty:
        print(f"\nAggregated DataFrame shape: {aggregated_data.shape}")
        print("Aggregated Data Head:")
        print(aggregated_data.head())

        try:
            aggregated_data.to_csv(OUTPUT_CSV_FILE)
            print(f"\nSuccessfully saved aggregated data to: {OUTPUT_CSV_FILE}")
        except Exception as e:
            print(f"\nError saving data to CSV: {e}")
    else:
        print("\nNo data was aggregated or a critical error occurred during fetching.")

    if failed_fetch_symbols:
        print("\n--- Symbols That Failed or Had Issues ---")
        for fsymbol in failed_fetch_symbols:
            print(fsymbol)

    print("\n--- Bulk Data Fetch Script Finished ---")