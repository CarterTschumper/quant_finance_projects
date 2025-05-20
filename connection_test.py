from ibapi.client import EClient
from ibapi.wrapper import EWrapper
# from ibapi.utils import iswrapper # Decorator not strictly needed for basic callbacks
import threading
import time

class IBConnectionTest(EWrapper, EClient):
    """ Simple class to test connection and receive basic callbacks """
    def __init__(self):
        EClient.__init__(self, self)
        # Use threading Event for signaling connection success/failure
        self._connection_acknowledged = threading.Event()
        self.next_valid_order_id = None

    # --- EWrapper callbacks ---

    def nextValidId(self, orderId: int):
        """ Called by TWS upon successful connection. """
        super().nextValidId(orderId)
        self.next_valid_order_id = orderId
        print(f"[Callback] Connection successful. Next Valid Order ID: {orderId}")
        # Signal that the connection is established and we have the ID
        self._connection_acknowledged.set()

    def error(self, reqId, errorCode, errorString, advancedOrderReject=""):
        """ Called by TWS for errors or informational messages. """
        super().error(reqId, errorCode, errorString, advancedOrderReject)
        # Log all errors and messages, decide handling based on code
        print(f"[Callback] Error/Message. Id: {reqId}, Code: {errorCode}, Msg: {errorString}, Advanced Order Reject: {advancedOrderReject}")

        # Specific handling for common connection issues
        # See: https://interactivebrokers.github.io/tws-api/message_codes.html
        if errorCode in [502, 504]: # Couldn't connect / Connection failed
            print(f"[Error] Failed to connect to TWS/Gateway. Is it running and API enabled on port?")
            self._connection_acknowledged.set() # Signal failure
        elif errorCode == 1100: # Connectivity between IB and TWS has been lost.
             print(f"[Error] Lost connectivity between IB and TWS/Gateway.")
             self._connection_acknowledged.set() # Signal failure/disconnection
        elif errorCode == 2104: # Market data farm connection is OK
             # This is an informational message, often received on connect
             print("[Info] Market data farm connection is OK.")
        elif errorCode in [2103, 2105, 2106, 2110, 2158]: # Other connectivity messages (often informational)
             print(f"[Info] Server Message Code: {errorCode}")
        elif errorCode == -1 and "Next Valid Order ID" in errorString:
             # Sometimes nextValidId also comes via error callback, ignore if already handled
             pass

    def connectionClosed(self):
        """ Called by EClient.__init__ when connection is closed """
        super().connectionClosed()
        print("[Info] Connection closed.")
        self._connection_acknowledged.set() # Also signal if connection closes unexpectedly

    # --- Methods for interaction ---

    def wait_for_connection(self, timeout=10):
        """ Waits for the connection acknowledgment signal. """
        print("Waiting for connection confirmation...")
        acknowledged = self._connection_acknowledged.wait(timeout=timeout)
        if not acknowledged:
            print(f"Connection attempt timed out after {timeout} seconds.")
        return acknowledged

    def is_connection_successful(self):
        """ Checks if nextValidId was received. """
        return self.next_valid_order_id is not None

# --- Main Execution Function ---

def run_connection_test():
    app = IBConnectionTest()

    # --- CONFIGURATION ---
    # !!! IMPORTANT: Verify and update these settings !!!
    host = "127.0.0.1"  # Standard for localhost

    # Use the port configured in your TWS API Settings
    # 7497 for TWS Paper Trading (default)
    # 7496 for TWS Live Trading (default)
    # 4002 for IB Gateway Paper Trading (default)
    # 4001 for IB Gateway Live Trading (default)
    tws_port = 7497     # <--- ****** CHECK AND CHANGE THIS ******

    client_id = 1       # Use a unique ID for this client connection
    # --- END CONFIGURATION ---

    print(f"Attempting to connect to TWS/Gateway on {host}:{tws_port} with Client ID {client_id}...")
    app.connect(host, tws_port, clientId=client_id)

    # Start the message processing loop in a separate thread
    # daemon=True allows the main program to exit even if this thread is running
    api_thread = threading.Thread(target=app.run, name="IB_API_Msg_Loop", daemon=True)
    api_thread.start()
    print("API message loop started in background thread.")

    # Wait for the connection attempt to resolve (success or failure)
    connection_acknowledged = app.wait_for_connection(timeout=10) # Wait up to 10 seconds

    if connection_acknowledged and app.is_connection_successful():
        print("-------------------------------------------------")
        print(">>> Basic Connection Test SUCCESSFUL <<<")
        print(f">>> Next Valid Order ID: {app.next_valid_order_id}")
        print("-------------------------------------------------")
        # You are connected! You could add requests here for further testing.
        # For now, just wait a moment before disconnecting.
        time.sleep(2)
    elif connection_acknowledged:
        print("-------------------------------------------------")
        print(">>> Basic Connection Test FAILED (Acknowledged Error) <<<")
        print(">>> Check error messages above. Ensure TWS/Gateway is running,")
        print(">>> API is enabled, port matches, and no other client uses ID 1.")
        print("-------------------------------------------------")
    else: # Timeout
        print("-------------------------------------------------")
        print(">>> Basic Connection Test FAILED (Timeout) <<<")
        print(">>> Check TWS/Gateway is running, API enabled, port matches,")
        print(">>> and 'Trusted IPs' settings if not connecting from localhost.")
        print("-------------------------------------------------")

    # Disconnect
    print("Disconnecting from TWS/Gateway...")
    app.disconnect()
    # Wait briefly for the thread to potentially process disconnect messages
    time.sleep(1)
    print("Program finished.")

# --- Run the test ---
if __name__ == "__main__":
    run_connection_test()