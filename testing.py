import pandas as pd
import numpy as np

df = pd.read_csv('data/IWV_holdings.csv', skiprows=9)
TICKER_COLUMN_NAME = 'Ticker'
EXCHANGE_COLUMN_NAME = 'Exchange'

EXCHANGE_MAPPING = {
    'NASDAQ': 'NASDAQ',
    'New York Stock Exchange Inc.': 'NYSE',
    'Cboe BZX': 'BATS',
    'Nyse Mkt Llc': 'AMEX'}

EXCHANGES_TO_DROP = [
    'NO MARKET (E.G. UNLISTED)',
    'Non-Nms Quotation Service (Nnqs)', # OTC/Pink - Drop unless you specifically want to try 'PINK'
    'Chicago Mercantile Exchange',       # Futures - Not STK
    'Index And Options Market',          # Options/Index - Not STK
]

print(f"Original DataFrame shape: {df.shape}")


# 3. Filter out rows with exchanges we want to drop
#    We use .isin() and negate it (~)
#    We need to handle potential 'nan' values - isin doesn't include nan,
#    so ~isin will *keep* nan unless we explicitly drop them.
#    Since we *want* to keep 'nan' and '-', we only drop the EXCHANGES_TO_DROP list.
df_filtered = df[~df[EXCHANGE_COLUMN_NAME].isin(EXCHANGES_TO_DROP)].copy()

print(f"Shape after dropping unusable exchanges: {df_filtered.shape}")

# 4. Apply the mapping to create a new 'PrimaryExchange' column
#    This will put 'NASDAQ', 'NYSE', etc., where a match is found,
#    and 'NaN' where there's no match (e.g., for '-', nan, or unmapped Cboe).
df_filtered['PrimaryExchange'] = df_filtered[EXCHANGE_COLUMN_NAME].map(EXCHANGE_MAPPING)

# 5. Extract the full list of symbols to fetch
#    This includes symbols with known exchanges AND those that will use 'SMART'
symbols_to_fetch = df_filtered[TICKER_COLUMN_NAME].dropna().unique().tolist()
symbols_to_fetch = [str(s).strip().upper() for s in symbols_to_fetch]
print(f"\nFound {len(symbols_to_fetch)} symbols to fetch.")

# 6. Create the Primary Exchange Map (ONLY for symbols with a known mapping)
#    We achieve this by dropping rows where 'PrimaryExchange' is NaN before creating the dict.
primary_exchange_map_df = df_filtered.dropna(subset=[TICKER_COLUMN_NAME, 'PrimaryExchange'])
primary_exchange_map = primary_exchange_map_df.set_index(TICKER_COLUMN_NAME)['PrimaryExchange'].to_dict()
# Ensure keys are strings and uppercase
primary_exchange_map = {str(k).strip().upper(): v for k, v in primary_exchange_map.items()}
print(f"Created primary exchange map for {len(primary_exchange_map)} symbols.")


# --- Output the Python Code ---

print("\n--- Python Code for SYMBOLS_TO_FETCH ---")
print("SYMBOLS_TO_FETCH = [")
for symbol in symbols_to_fetch:
    print(f'    "{symbol}",')
print("]")

print("\n--- Python Code for PRIMARY_EXCHANGE_MAP ---")
print("PRIMARY_EXCHANGE_MAP = {")
for symbol, exchange in primary_exchange_map.items():
    print(f'    "{symbol}": "{exchange}",')
print("}")

# You can now use these 'symbols_to_fetch' and 'primary_exchange_map'
# in your bulk_daily_data_fetcher.py script.

import pandas as pd

# --- (Your previous code to load and filter 'df' goes here) ---

# Define the output path
SYMBOLS_CSV_PATH = 'data/symbols_for_fetcher.csv' 

# Ensure 'Ticker' is uppercase
df_filtered['Ticker'] = df_filtered['Ticker'].str.strip().str.upper()

# Select only Ticker and PrimaryExchange, drop duplicates
output_df = df_filtered[['Ticker', 'PrimaryExchange']].drop_duplicates(subset=['Ticker']).copy()

# Save to CSV
try:
    output_df.to_csv(SYMBOLS_CSV_PATH, index=False)
    print(f"\n---> Saved symbols and exchanges to: {SYMBOLS_CSV_PATH}")
except Exception as e:
    print(f"\n---> ERROR saving symbols CSV: {e}")