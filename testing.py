# testing.py
"""
Test script to fetch historical data for various asset classes using ib_functions.py
to identify accessible data and potential subscription needs.
"""

import pandas as pd
import time

# Import functions from our custom modules
from ib_functions import get_historical_data # We only need this for historical data tests

# --- Configuration ---
# !!! IMPORTANT: Verify this port matches your TWS API settings !!!
TWS_PORT = 7497 # Default for TWS Paper Trading
STARTING_CLIENT_ID = 701 # Starting client ID for this test run

# --- Test Cases Definition ---
# Each item is a dictionary defining the parameters for get_historical_data
# and a descriptive name for the test.
test_cases = [
    # --- US ETFs ---
    {
        "name": "US ETF: QQQ (NASDAQ)",
        "params": {"symbol": "QQQ", "sec_type": "ETF", "exchange": "SMART", "primary_exchange": "NASDAQ", "currency": "USD", "duration": "1 M", "bar_size": "1 day", "what_to_show": "ADJUSTED_LAST"}
    },
    {
        "name": "US ETF: SPY (ARCA)",
        "params": {"symbol": "SPY", "sec_type": "ETF", "exchange": "ARCA", "currency": "USD", "duration": "1 M", "bar_size": "1 day", "what_to_show": "ADJUSTED_LAST"}
    },
    # --- European ETFs ---
    {
        "name": "European ETF: EXS1 (DAX UCITS on XETRA/IBIS2)", # IBIS2 is often the API exchange for XETRA
        "params": {"symbol": "EXS1", "sec_type": "ETF", "exchange": "IBIS2", "currency": "EUR", "duration": "1 M", "bar_size": "1 day", "what_to_show": "TRADES"} # Adjusted_last might not be available for all non-US
    },
    {
        "name": "European ETF: CSPX (S&P 500 UCITS on LSE)",
        "params": {"symbol": "CSPX", "sec_type": "ETF", "exchange": "LSEETF", "currency": "USD", "duration": "1 M", "bar_size": "1 day", "what_to_show": "TRADES"}
    },
    {
        "name": "European ETF: EUNK (Euro Stoxx 50 UCITS on Euronext Amsterdam)",
        "params": {"symbol": "EUNK", "sec_type": "ETF", "exchange": "AEB", "currency": "EUR", "duration": "1 M", "bar_size": "1 day", "what_to_show": "TRADES"}
    },
    # --- Forex ---
    {
        "name": "Forex: EUR.USD",
        "params": {"symbol": "EUR", "sec_type": "CASH", "exchange": "IDEALPRO", "currency": "USD", "duration": "5 D", "bar_size": "1 hour", "what_to_show": "MIDPOINT"}
    },
    {
        "name": "Forex: GBP.JPY",
        "params": {"symbol": "GBP", "sec_type": "CASH", "exchange": "IDEALPRO", "currency": "JPY", "duration": "5 D", "bar_size": "1 hour", "what_to_show": "MIDPOINT"}
    },
    # --- Cryptocurrencies (Requires market data subscriptions on IBKR) ---
    {
        "name": "Crypto: BTC.USD (PAXOS)",
        "params": {"symbol": "BTC", "sec_type": "CRYPTO", "exchange": "PAXOS", "currency": "USD", "duration": "10 D", "bar_size": "1 day", "what_to_show": "TRADES"}
    },
    {
        "name": "Crypto: ETH.USD (PAXOS)",
        "params": {"symbol": "ETH", "sec_type": "CRYPTO", "exchange": "PAXOS", "currency": "USD", "duration": "10 D", "bar_size": "1 day", "what_to_show": "TRADES"}
    },
    {
        "name": "Crypto: BTC.USD (GEMINI)", # Alternative exchange
        "params": {"symbol": "BTC", "sec_type": "CRYPTO", "exchange": "GEMINI", "currency": "USD", "duration": "10 D", "bar_size": "1 day", "what_to_show": "TRADES"}
    },
    # --- Stocks on International Exchanges ---
    {
        "name": "UK Stock: VOD (Vodafone on LSE)",
        "params": {"symbol": "VOD", "sec_type": "STK", "exchange": "LSE", "currency": "GBP", "duration": "1 M", "bar_size": "1 day", "what_to_show": "TRADES"} # Pence (GBX) might be default, check contract
    },
    {
        "name": "Canadian Stock: RY (Royal Bank on TSE)",
        "params": {"symbol": "RY", "sec_type": "STK", "exchange": "TSE", "currency": "CAD", "duration": "1 M", "bar_size": "1 day", "what_to_show": "TRADES"}
    },
    # --- Indices (Often require specific subscriptions) ---
    {
        "name": "Index: SPX (S&P 500 Index)",
        "params": {"symbol": "SPX", "sec_type": "IND", "exchange": "CBOE", "currency": "USD", "duration": "1 M", "bar_size": "1 day", "what_to_show": "TRADES"}
    },
    {
        "name": "Index: VIX (Volatility Index)",
        "params": {"symbol": "VIX", "sec_type": "IND", "exchange": "CBOE", "currency": "USD", "duration": "1 M", "bar_size": "1 day", "what_to_show": "TRADES"}
    },
    # --- Test for known problematic request (wrong exchange for crypto) ---
    {
        "name": "Problematic Test: BTC on SMART Exchange (Incorrect)",
        "params": {"symbol": "BTC", "sec_type": "CRYPTO", "exchange": "SMART", "currency": "USD", "duration": "1 D", "bar_size": "1 day", "what_to_show": "TRADES"}
    },
    # --- Test for a potentially non-existent symbol ---
    {
        "name": "Non-Existent Symbol Test: NONEXISTENTXYZ",
        "params": {"symbol": "NONEXISTENTXYZ", "sec_type": "STK", "exchange": "SMART", "primary_exchange":"NASDAQ", "currency": "USD", "duration": "1 D", "bar_size": "1 day", "what_to_show": "TRADES"}
    }
]

# --- Main Test Loop ---
current_client_id = STARTING_CLIENT_ID

print("Starting diverse asset historical data fetch tests...")
print(f"Using TWS/Gateway Port: {TWS_PORT}")
print("Ensure TWS/Gateway is running and API is enabled.\n")

for test_case in test_cases:
    print(f"--- Running Test: {test_case['name']} ---")
    print(f"Parameters: {test_case['params']}")

    # Add port and unique client_id to params for the function call
    current_params = test_case['params'].copy()
    current_params['port'] = TWS_PORT
    current_params['client_id'] = current_client_id

    data_df = get_historical_data(**current_params)

    if data_df is not None and not data_df.empty:
        print(f"[SUCCESS] Fetched {len(data_df)} bars for {test_case['name']}.")
        print("Data Head:")
        print(data_df.head())
        print("\nData Tail:")
        print(data_df.tail())
    elif data_df is not None and data_df.empty:
        print(f"[NO DATA] Request for {test_case['name']} was successful but returned no data (empty DataFrame).")
        print("This might be due to no trading activity for the period, incorrect contract parameters, or lack of market data subscription for this specific instrument/exchange combination.")
    else: # data_df is None
        print(f"[FAILED] Failed to fetch data for {test_case['name']}.")
        print("Check console output from 'ib_functions.py' for specific IBKR error codes and messages.")
        print("Common reasons for failure include:")
        print("  - TWS/Gateway not running or API not enabled on the correct port.")
        print("  - Incorrect contract details (symbol, sec_type, exchange, currency).")
        print("  - Lack of market data subscriptions (Error codes like 162, 200, 354).")
        print("  - Pacing violations (too many requests too quickly - though script has sleeps).")
        print("  - Ambiguous contract (Error code 200 often indicates this - try adding primary_exchange).")

    print("-" * 50 + "\n")
    current_client_id += 1 # Increment client ID for the next test
    time.sleep(5) # PAUSE between API calls to respect pacing limits

print("--- All Tests Finished ---")
