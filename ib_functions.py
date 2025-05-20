# ib_functions.py
"""
Module containing helper functions to interact with the Interactive Brokers API,
simplifying common tasks like fetching historical and fundamental data.
"""

import threading
import time
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import pandas as pd
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import BarData, TickerId
import xml.etree.ElementTree as ET


class IBDataApp(EWrapper, EClient):
    """
    Handles the connection, requests, and reception of data (historical, fundamental)
    from IBKR TWS/Gateway.
    """
    def __init__(self):
        EClient.__init__(self, self)
        self.historical_data: Dict[TickerId, List[BarData]] = {}
        self.fundamental_data: Dict[TickerId, str] = {}
        self._request_complete_events: Dict[TickerId, threading.Event] = {}
        self._connection_event = threading.Event()
        self._error_event = threading.Event()
        self.next_valid_order_id: Optional[int] = None
        self._next_req_id: TickerId = 0
        self._error_message_for_request: Dict[TickerId, str] = {}
        self._general_error_message: Optional[str] = None

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.next_valid_order_id = orderId
        print(f"[IB App] Connection successful. Next Valid Order ID: {orderId}")
        self._connection_event.set()

    def error(self, reqId: TickerId, errorCode: int, errorString: str, advancedOrderRejectJson=""):
        super().error(reqId, errorCode, errorString, advancedOrderRejectJson)
        log_message = f"[IB App] TWS Message. Id: {reqId}, Code: {errorCode}, Msg: \"{errorString}\""
        common_info_codes = [2104, 2106, 2107, 2103, 2158]
        if not (reqId == -1 and errorCode in common_info_codes):
            print(log_message)
        if reqId == -1 and errorCode in common_info_codes:
            return

        critical_failure_codes = [
            502, 504, 507, 509, 522, 1100, 1101, 1102, 1300, 2100, 2101, 2105,
            2110, 2150, 2157, 326, 100, 101, 102, 103, 110, 162, 165, 200, 203,
            300, 309, 317, 320, 321, 322, 354, 366, 388, 400, 404, 430, 501, 503, 505
        ]
        if errorCode in critical_failure_codes or errorCode == 325: # 325 for fundamental data subscription
            error_to_store = f"Error (Code: {errorCode}, ReqId: {reqId}): {errorString}"
            if reqId != -1:
                self._error_message_for_request[reqId] = error_to_store
            else:
                self._general_error_message = error_to_store
            if errorCode in [502, 504, 507, 509, 522, 1100, 1101, 1102, 1300, 2100, 2101, 2105, 2110, 2150, 2157, 326]:
                self._error_event.set()
                self._connection_event.set()
            if reqId != -1 and reqId in self._request_complete_events:
                self._request_complete_events[reqId].set()

    def historicalData(self, reqId: TickerId, bar: BarData):
        if reqId not in self.historical_data:
            self.historical_data[reqId] = []
        self.historical_data[reqId].append(bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        print(f"[IB App] HistoricalDataEnd. ReqId: {reqId} from {start} to {end}")
        if reqId in self._request_complete_events:
            self._request_complete_events[reqId].set()

    def fundamentalData(self, reqId: TickerId, data: str):
        print(f"[IB App] FundamentalData Received. ReqId: {reqId}, XML Length: {len(data)}")
        self.fundamental_data[reqId] = data
        if reqId in self._request_complete_events:
            self._request_complete_events[reqId].set()

    def connectionClosed(self):
        super().connectionClosed()
        print("[IB App] Connection closed.")
        self._general_error_message = "Connection closed by TWS/Gateway."
        self._error_event.set()
        self._connection_event.set()

    def get_next_req_id(self) -> TickerId:
        if self._next_req_id == 0:
             self._next_req_id = int(time.time() * 10) % 10000 + 500 # Ensure unique start
        req_id = self._next_req_id
        self._next_req_id += 1
        return req_id

    def run_loop(self):
        print("[IB App] Starting message loop.")
        self.run()
        print("[IB App] Message loop finished.")

    def request_historical_data_internal(self, contract: Contract, durationStr: str, barSizeSetting: str, whatToShow: str, useRTH: int, formatDate: int) -> Optional[pd.DataFrame]:
        req_id = self.get_next_req_id()
        print(f"[IB App] Requesting historical data for ReqId: {req_id}, Contract: {contract.symbol} ({contract.secType}) on {contract.exchange}")

        completion_event = threading.Event()
        self._request_complete_events[req_id] = completion_event
        self.historical_data[req_id] = []
        self._error_message_for_request.pop(req_id, None)

        self.reqHistoricalData(
            reqId=req_id, contract=contract, endDateTime="",
            durationStr=durationStr, barSizeSetting=barSizeSetting,
            whatToShow=whatToShow, useRTH=useRTH, formatDate=formatDate,
            keepUpToDate=False, chartOptions=[]
        )

        print(f"[IB App] Waiting for historical data response for ReqId: {req_id}...")
        timeout_seconds = 60
        completed = completion_event.wait(timeout=timeout_seconds)

        if req_id in self._request_complete_events: del self._request_complete_events[req_id]

        request_specific_error = self._error_message_for_request.pop(req_id, None)
        request_bars = self.historical_data.pop(req_id, [])

        if self._error_event.is_set() and not request_bars:
            print(f"[IB App Error] Critical error occurred. General Error: {self._general_error_message}")
            return None
        if request_specific_error and not request_bars:
            print(f"[IB App Error] Failed to get historical data for ReqId {req_id}. Specific Error: {request_specific_error}")
            return None
        if not completed and not request_bars:
            print(f"[IB App Error] Historical data request {req_id} timed out after {timeout_seconds} seconds and no data received.")
            self.cancelHistoricalData(req_id)
            return None
        if not request_bars:
             print(f"[IB App Warning] No historical data bars received for ReqId {req_id}.")
             return pd.DataFrame()

        try:
            df = pd.DataFrame([vars(bar) for bar in request_bars])
        except Exception as e:
            print(f"[IB App Error] Failed to convert BarData to DataFrame for ReqId {req_id}: {e}")
            return pd.DataFrame()

        if df.empty: return df

        try:
            # --- DateTime Parsing ---
            date_col_series = df['date']
            if formatDate == 2: # Epoch timestamp (typically for intraday bars < 1 day)
                # Ensure it's treated as string first, then int for pd.to_datetime
                df['date'] = pd.to_datetime(date_col_series.astype(str).astype(int), unit='s', utc=True)
                # Optionally convert to a specific timezone, e.g., New York if desired
                # try:
                #     df['date'] = df['date'].dt.tz_convert('America/New_York')
                # except Exception as tz_e:
                #     print(f"[IB App Warning] Could not convert epoch to America/New_York for ReqId {req_id}: {tz_e}")
            elif formatDate == 1: # String format: YYYYMMDD or YYYYMMDD  HH:MM:SS
                # Normalize by removing potential double spaces before parsing
                normalized_dates = date_col_series.astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
                # Attempt to parse, trying common formats
                try:
                    # Try YYYYMMDD HH:MM:SS first for intraday
                    df['date'] = pd.to_datetime(normalized_dates, format='%Y%m%d %H:%M:%S', errors='raise')
                except ValueError:
                    try:
                        # Fallback to YYYYMMDD for daily
                        df['date'] = pd.to_datetime(normalized_dates, format='%Y%m%d', errors='raise')
                    except ValueError:
                        print(f"[IB App Warning] Failed to parse date strings with known formats for ReqId {req_id}. Dates: {date_col_series.head().tolist()}")
                        df['date'] = pd.to_datetime(normalized_dates, errors='coerce') # Last resort, coerce errors
            else:
                print(f"[IB App Warning] Unknown formatDate '{formatDate}' for ReqId {req_id}. Attempting generic parsing.")
                df['date'] = pd.to_datetime(date_col_series.astype(str).str.replace(r'\s+', ' ', regex=True).str.strip(), errors='coerce')


            if df['date'].isnull().any():
                 print(f"[IB App Warning] Some dates resulted in NaT after parsing for ReqId {req_id}. Original head: {date_col_series.head().tolist()}")

            df.rename(columns={'date': 'DateTime', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume', 'barCount': 'BarCount', 'average': 'WAP'}, inplace=True)
            df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0).astype('int64')
            df.set_index('DateTime', inplace=True)
            columns_to_keep = ['Open', 'High', 'Low', 'Close', 'Volume', 'WAP', 'BarCount']
            available_columns = [col for col in columns_to_keep if col in df.columns]
            df = df[available_columns]
        except Exception as e:
            print(f"[IB App Error] Failed during DataFrame formatting for ReqId {req_id}: {e}")
            return df

        print(f"[IB App] Successfully processed {len(df)} bars for ReqId: {req_id}.")
        return df

    def request_fundamental_data_internal(self, contract: Contract, report_type: str) -> Optional[str]:
        req_id = self.get_next_req_id()
        print(f"[IB App] Requesting fundamental data for ReqId: {req_id}, Contract: {contract.symbol}, Report: {report_type}")
        completion_event = threading.Event()
        self._request_complete_events[req_id] = completion_event
        self.fundamental_data.pop(req_id, None)
        self._error_message_for_request.pop(req_id, None)

        self.reqFundamentalData(reqId=req_id, contract=contract, reportType=report_type, fundamentalDataOptions=[])
        print(f"[IB App] Waiting for fundamental data response for ReqId: {req_id}...")
        timeout_seconds = 45
        completed = completion_event.wait(timeout=timeout_seconds)
        if req_id in self._request_complete_events: del self._request_complete_events[req_id]

        request_specific_error = self._error_message_for_request.pop(req_id, None)
        result_xml = self.fundamental_data.pop(req_id, None)

        if self._error_event.is_set() and not result_xml:
            print(f"[IB App Error] Critical error occurred. General Error: {self._general_error_message}")
            return None
        if request_specific_error and not result_xml:
            print(f"[IB App Error] Failed to get fundamental data for ReqId {req_id}. Specific Error: {request_specific_error}")
            return None
        if not completed and not result_xml:
            print(f"[IB App Error] Fundamental data request {req_id} timed out after {timeout_seconds} seconds.")
            self.cancelFundamentalData(req_id)
            return None
        if not result_xml:
             print(f"[IB App Warning] No fundamental data XML received for ReqId {req_id}.")
             return None
        print(f"[IB App] Successfully received fundamental data XML for ReqId: {req_id}.")
        return result_xml

def _create_connection(host: str, port: int, client_id: int) -> Optional[IBDataApp]:
    app = IBDataApp()
    print(f"Initiating connection to TWS/Gateway on {host}:{port} with Client ID {client_id}...")
    app.connect(host, port, clientId=client_id)
    api_thread = threading.Thread(target=app.run_loop, name=f"IB_API_Thread_{client_id}", daemon=True)
    api_thread.start()
    app.api_thread = api_thread
    connection_timeout = 15
    print(f"Waiting up to {connection_timeout}s for connection confirmation...")
    connected = app._connection_event.wait(timeout=connection_timeout)
    if not connected or app._error_event.is_set() or app.next_valid_order_id is None:
        error_msg = app._general_error_message or "Connection timed out or failed before nextValidId received."
        print(f"Failed to connect to IBKR: {error_msg}")
        if app.isConnected(): app.disconnect()
        return None
    print("Connection successful.")
    return app

def _disconnect_connection(app: Optional[IBDataApp]):
    if app and app.isConnected():
        print("Disconnecting from TWS/Gateway...")
        app.disconnect()
        time.sleep(1)
        if hasattr(app, 'api_thread') and app.api_thread.is_alive():
            print("[Warning] API thread still alive after disconnect request.")
    print("IBKR function finished execution.")

def get_historical_data(
    symbol: str,
    sec_type: str = "STK",
    exchange: str = "SMART",
    primary_exchange: Optional[str] = None,
    currency: str = "USD",
    duration: str = "1 M",
    bar_size: str = "1 day",
    what_to_show: Optional[str] = None,
    use_rth: bool = True,
    format_date_setting: int = 1, # Default to YYYYMMDD HH:MM:SS string
    host: str = "127.0.0.1",
    port: int = 7497,
    client_id: int = 201
) -> Optional[pd.DataFrame]:
    app = _create_connection(host, port, client_id)
    if not app: return None
    df_historical = None
    try:
        actual_sec_type = sec_type
        if sec_type == "ETF":
            print(f"[Info] For sec_type 'ETF', attempting with 'STK' as it's often expected by IBKR API for historical data.")
            actual_sec_type = "STK"

        if what_to_show is None:
            if actual_sec_type in ["STK", "ETF"]:
                what_to_show = "ADJUSTED_LAST"
            elif actual_sec_type == "CRYPTO":
                what_to_show = "AGGTRADES"
            else: # CASH, IND, FUT, etc.
                what_to_show = "TRADES" # Or MIDPOINT for CASH if preferred default
        print(f"Requesting data type: {what_to_show}")

        contract = Contract()
        contract.symbol = symbol
        contract.secType = actual_sec_type
        contract.currency = currency
        contract.exchange = exchange
        if primary_exchange: contract.primaryExchange = primary_exchange

        if contract.secType == "CASH":
            contract.exchange = "IDEALPRO"
            # For CASH, if bar_size is intraday (e.g., '1 hour', '1 min'), formatDate=2 (epoch) is often more reliable
            if "min" in bar_size or "hour" in bar_size or "sec" in bar_size:
                print("[Info] For intraday CASH (Forex) data, overriding formatDate to 2 (epoch).")
                format_date_setting = 2
        elif contract.secType == "CRYPTO":
            if exchange == "SMART":
                 print("[Error] SMART exchange is not valid for CRYPTO. Specify PAXOS, GEMINI, etc.")
                 return None
            if what_to_show == "TRADES": # Correcting for crypto if user didn't override
                print("[Info] For CRYPTO, changing what_to_show from TRADES to AGGTRADES.")
                what_to_show = "AGGTRADES"
        elif contract.secType in ["STK"] and exchange == "SMART" and not primary_exchange:
             print(f"[Warning] For {contract.secType} on SMART, specifying primary_exchange is recommended.")

        df_historical = app.request_historical_data_internal(
            contract=contract, durationStr=duration, barSizeSetting=bar_size,
            whatToShow=what_to_show, useRTH=1 if use_rth else 0, formatDate=format_date_setting
        )
    except Exception as e:
        print(f"An unexpected error occurred in get_historical_data: {e}")
        import traceback
        traceback.print_exc()
        df_historical = None
    finally:
        _disconnect_connection(app)
    return df_historical

def get_fundamental_data(
    symbol: str,
    sec_type: str = "STK",
    exchange: str = "SMART",
    primary_exchange: Optional[str] = None,
    currency: str = "USD",
    report_type: str = "ReportsFinSummary",
    host: str = "127.0.0.1",
    port: int = 7497,
    client_id: int = 301
) -> Optional[str]:
    app = _create_connection(host, port, client_id)
    if not app: return None
    result_xml = None
    try:
        contract = Contract()
        contract.symbol = symbol
        contract.secType = sec_type
        contract.currency = currency
        contract.exchange = exchange
        if primary_exchange: contract.primaryExchange = primary_exchange
        if sec_type != "STK":
             print(f"[Warning] Fundamental data typically only available for STK secType.")
        if exchange == "SMART" and not primary_exchange:
             print(f"[Warning] For STK on SMART, specifying primary_exchange is highly recommended.")
        result_xml = app.request_fundamental_data_internal(
            contract=contract, report_type=report_type
        )
    except Exception as e:
        print(f"An unexpected error occurred in get_fundamental_data: {e}")
        import traceback
        traceback.print_exc()
        result_xml = None
    finally:
        _disconnect_connection(app)
    return result_xml

def parse_fundamental_snapshot(xml_string: Optional[str]) -> Optional[Dict[str, Any]]:
    if not xml_string: return None
    try:
        root = ET.fromstring(xml_string)
        snapshot = {}
        ratios_section = root.find(".//Ratios")
        if ratios_section is not None:
            snapshot['Ratios'] = {}
            for ratio in ratios_section.findall('Ratio'):
                field_name = ratio.attrib.get('FieldName')
                value = ratio.text
                if field_name:
                    try: snapshot['Ratios'][field_name] = float(value) if value else None
                    except (ValueError, TypeError): snapshot['Ratios'][field_name] = value
        co_general_info = root.find(".//CoGeneralInfo")
        if co_general_info is not None:
             snapshot['CompanyName'] = co_general_info.attrib.get('CompanyName')
             snapshot['Country'] = co_general_info.attrib.get('Country')
        address = root.find(".//Address")
        if address is not None:
             snapshot['Address'] = {k: v for k, v in address.attrib.items() if v}
        return snapshot if snapshot else None
    except ET.ParseError as e:
        print(f"Error parsing fundamental XML: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error during XML parsing: {e}")
        return None

if __name__ == "__main__":
    TWS_PAPER_PORT = 7497
    print("Running extended example usage (with v6 fixes)...")

    # --- Test Forex with automatic formatDate=2 for intraday ---
    print("\n--- Example: EUR.USD Forex 1 Minute (auto formatDate=2) ---")
    eurusd_data_epoch = get_historical_data(
        symbol="EUR", sec_type="CASH", currency="USD", duration="1 D",
        bar_size="1 min", what_to_show="MIDPOINT", use_rth=False,
        # formatDate will be automatically set to 2 by get_historical_data for intraday CASH
        port=TWS_PAPER_PORT, client_id=601 # New client ID
    )
    if eurusd_data_epoch is not None:
        print(f"EUR/USD Data (First 5 rows):\n{eurusd_data_epoch.head()}")
        print(f"EUR/USD Data Info:\n")
        eurusd_data_epoch.info()
    else:
        print("EUR/USD data fetch failed or returned None.")


    time.sleep(3)

    # --- Test ETF with sec_type="ETF" (function will try "STK") ---
    print("\n--- Example: QQQ US ETF Daily (passing sec_type='ETF') ---")
    qqq_data = get_historical_data(
        symbol="QQQ", sec_type="ETF", exchange="SMART", primary_exchange="NASDAQ",
        currency="USD", duration="1 M", bar_size="1 day",
        port=TWS_PAPER_PORT, client_id=602
    )
    if qqq_data is not None:
        print(f"QQQ Data (First 5 rows):\n{qqq_data.head()}")
        print(f"QQQ Data Info:\n")
        qqq_data.info()
    else:
        print("QQQ data fetch failed or returned None.")


    time.sleep(3)

    # --- Test Crypto with corrected what_to_show ---
    print("\n--- Example: BTC/USD Crypto Daily (PAXOS) ---")
    btc_data = get_historical_data(
        symbol="BTC", sec_type="CRYPTO", exchange="PAXOS", currency="USD",
        duration="10 D", bar_size="1 day", # what_to_show will default to AGGTRADES
        port=TWS_PAPER_PORT, client_id=603
    )
    if btc_data is not None:
        if not btc_data.empty:
            print(f"BTC/USD Data (First 5 rows):\n{btc_data.head()}")
        else:
            print("BTC/USD DataFrame is empty (check subscriptions for PAXOS CRYPTO).")
    else:
        print("BTC/USD request failed or returned None (check subscriptions for PAXOS CRYPTO).")

    print("\nExtended example usage finished.")
