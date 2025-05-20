# Quant Finance Projects

A collection of Python-based quantitative finance models and tools. This project aims to cover various aspects of quantitative analysis, including risk metrics calculation and potentially developing quantitative trading strategies using the Interactive Brokers API.

**Current features/modules include:**
- Risk metrics calculations (`risk_metrics.py`)
- Example usage of risk metrics (`testing.py`)
- Simple options pricing algorithms (black_scholes.py, GMB_pricing.py, test_options.py)

The code is designed to be used within a Python virtual environment.

## Project Setup and IBKR API Integration

This section details how to set up the project environment, particularly for integrating with the Interactive Brokers (IBKR) API for data and trading. These instructions are primarily for a Windows setup.

### 1. Prerequisites
* Python 3.x installed.
* Access to an Interactive Brokers account (live or paper) for API usage.

### 2. Install Interactive Brokers Software
    * Download and install **Trader Workstation (TWS)** from the IBKR website.
    * Download the latest **"stable msi for windows" TWS API software** from [http://interactivebrokers.github.io/](http://interactivebrokers.github.io/). As of the last update to this guide, the version used was `10.30.1`, but you should download the latest stable version.
    * Run the API MSI installer. This typically installs the API source files to `C:\TWS API`.

### 3. Set Up Python Project Environment
    * Clone or copy this project folder (`quant_finance_projects`) to your machine.
    * Open a terminal (like PowerShell, Command Prompt, or the VS Code integrated terminal) in the project's root directory.
    * Create a Python virtual environment:
        ```bash
        python -m venv .venv
        ```
    * Activate the virtual environment:
        * On Windows:
            ```bash
            .venv\Scripts\activate
            ```
        * On macOS/Linux:
            ```bash
            source .venv/bin/activate
            ```

### 4. Manually Install IBKR API Python Client (`ibapi`)
The `ibapi` Python client is installed from the source files provided by the TWS API software, not directly from PyPI for the latest official version.
    * In your **activated terminal**, navigate to the API source directory created by the MSI installer:
        ```bash
        cd "C:\TWS API\source\pythonclient"
        ```
    * Ensure the `setuptools` and `wheel` packages are available, then build and install `ibapi`:
        ```bash
        pip install setuptools wheel
        python setup.py bdist_wheel
        ```
    * Install the built package. **Note:** The `VERSION` in the filename below (e.g., `10.30.1`) will correspond to the API version you downloaded. Check the `dist` subfolder in `C:\TWS API\source\pythonclient\` for the exact `.whl` filename.
        ```bash
        pip install dist\ibapi-VERSION-py3-none-any.whl 
        ```
        (For example: `pip install dist\ibapi-10.30.1-py3-none-any.whl`)

### 5. Install Other Python Dependencies
    * Navigate back to your project's root directory in the terminal (where `requirements.txt` is located).
    * With the virtual environment still active, install the requirements:
        ```bash
        pip install -r requirements.txt
        ```
        *(This will install `scipy`, `numpy`, `matplotlib`, `yfinance`, `pandas`, `beautifulsoup4`, `requests`, etc., as defined in your `requirements.txt` file. It will also recognize that `ibapi` is already installed if the versions match the local file reference in `requirements.txt`.)*

### 6. Configure Trader Workstation (TWS) for API Access
    * Launch Trader Workstation (TWS) and log in (to your paper or live account).
    * Go to `File > Global Configuration`.
    * In the left pane, expand `API` and select `Settings`.
    * Ensure "**Enable ActiveX and Socket Clients**" is checked.
    * Note the "**Socket port**" (e.g., `7497` for paper trading with TWS, `7496` for live trading with TWS). You will need this port number in your Python scripts.
    * If you intend to place orders or perform actions other than just reading data via the API, ensure "**Read-Only API**" is **unchecked**. For initial data retrieval and testing, it can be left checked for safety.
    * Optionally, for convenience if your script runs on the same machine as TWS, add `127.0.0.1` to "**Trusted IP Addresses**" to prevent connection confirmation prompts.
    * Click "Apply" and "OK".

Your project environment should now be ready to develop and run Python scripts interacting with the IBKR API.

---

*(You can re-add or keep your previous list of models like Black-Scholes etc. at the top if they are distinct components of the project. I've tried to make the intro a bit more general and then focus on the IBKR setup as it was our main discussion point.)*