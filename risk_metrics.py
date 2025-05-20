# risk_metrics.py
import pandas as pd
import numpy as np
import scipy.stats as stats


def calculate_log_returns(price_series, period=1):
    """Calculates log returns for a given price series."""
    if not isinstance(price_series, pd.Series):
        price_series = pd.Series(price_series)
    return np.log(price_series / price_series.shift(period))


def ParametricVaR(returns, alpha, horizon=1):
    """Calculates Parametric Value at Risk (VaR) assuming normal distribution."""
    # For a loss, we look at the left tail, so ppf(alpha)
    z = stats.norm.ppf(alpha)
    mean = np.nanmean(returns)
    std = np.nanstd(returns)
    # VaR is potential loss, so it's positive. mean + z*std is the point, so -(mean + z*std)
    var_value = -(mean + z * std)
    return var_value * np.sqrt(horizon)


def HistoricalVaR(returns, alpha, horizon=1, rolling=False, window_size=250):
    """Calculates Historical Value at Risk (VaR)."""
    if rolling:
        # quantile(alpha) gives the value at the alpha-th percentile (e.g., 5th percentile for alpha=0.05)
        # VaR is a loss, so we take the negative of this value if it's from the lower tail of returns.
        return -returns.rolling(window=window_size).quantile(alpha)
    else:
        return -returns.quantile(alpha) * np.sqrt(horizon)


def ParametricCVaR(returns, alpha, horizon=1):
    """Calculates Parametric Conditional Value at Risk (CVaR) assuming normal distribution."""
    z = stats.norm.ppf(alpha) # Z-score for the alpha percentile
    mean = np.nanmean(returns)
    std = np.nanstd(returns)
    # CVaR (Expected Shortfall) = - (mean + std * pdf(z) / alpha)
    # This formula gives the expected value of returns below the VaR point.
    cvar_value = -(mean + std * stats.norm.pdf(z) / alpha)
    return cvar_value * np.sqrt(horizon)


def HistoricalCVaR(returns, alpha, horizon=1, rolling=False, window_size=250):
    """Calculates Historical Conditional Value at Risk (CVaR)."""
    if rolling:
        return returns.rolling(window=window_size).apply(
            lambda x: -x[x <= x.quantile(alpha)].mean(), raw=False # raw=False for quantile on Series
        )
    else:
        threshold = returns.quantile(alpha) # This is the VaR value (typically negative or small for losses)
        tail_losses = returns[returns <= threshold]
        if tail_losses.empty:
            return 0.0 # Or np.nan, or handle as appropriate
        # CVaR is reported as a positive loss value
        return -tail_losses.mean() * np.sqrt(horizon)


def MaxDrawdown(returns_series: pd.Series):
    """
    Calculates the Maximum Drawdown (MDD) from a pandas Series of returns.
    The returns should be fractional (e.g., 0.01 for 1%), not log returns for this specific formulation.
    If using log returns, the interpretation of cumsum and exp changes.
    This version assumes returns_series are arithmetic/fractional returns.

    Args:
        returns_series (pd.Series): A pandas Series of arithmetic returns, indexed by date/time.

    Returns:
        tuple: A tuple containing:
            - max_drawdown (float): The maximum drawdown percentage (e.g., 0.2 means 20% drawdown, positive).
            - max_drawdown_date (pd.Timestamp or None): The date when the maximum drawdown trough occurred.
    """
    if not isinstance(returns_series, pd.Series) or returns_series.empty:
        return 0.0, None

    # Calculate a wealth index (cumulative product of 1 + returns)
    # Start with 1, then multiply by (1 + r_t) for each period
    # Fill NaNs in returns with 0 so they don't break the product (1+0 = 1)
    wealth_index = (1 + returns_series.fillna(0)).cumprod()

    # Calculate previous peaks
    previous_peaks = wealth_index.cummax()

    # Calculate drawdown series (percentage from peak)
    # Drawdown = (Current Value - Peak Value) / Peak Value  OR  Current Value / Peak Value - 1
    # This will be negative or zero.
    drawdown_series = (wealth_index - previous_peaks) / previous_peaks
    # Alternative: drawdown_series = wealth_index / previous_peaks - 1

    # Maximum drawdown is the minimum value in the drawdown series (most negative)
    max_dd_value = drawdown_series.min()
    if pd.isna(max_dd_value): # Handle case where all returns might be NaN leading to NaN MDD
        return 0.0, None

    # MDD is usually reported as a positive percentage
    max_drawdown_positive = -max_dd_value

    # Find the date (index) when this maximum drawdown occurred (trough of the drawdown)
    max_drawdown_date = drawdown_series.idxmin() if not drawdown_series.empty else None

    return max_drawdown_positive, max_drawdown_date


def MeanOverStd(returns):
    """Calculates the ratio of mean return to standard deviation (non-annualized Sharpe)."""
    mean_ret = np.nanmean(returns)
    std_dev = np.nanstd(returns)
    if std_dev == 0: return np.inf if mean_ret > 0 else (-np.inf if mean_ret < 0 else 0.0)
    return mean_ret / std_dev


def CAGR(returns_series: pd.Series):
    """
    Calculates Compound Annual Growth Rate from a Series of arithmetic returns.
    Assumes returns_series index can be used to determine the number of years.
    """
    if not isinstance(returns_series, pd.Series) or returns_series.empty:
        return 0.0
    
    clean_returns = returns_series.dropna()
    if clean_returns.empty:
        return 0.0

    # Calculate total return over the period
    # (1+r1)*(1+r2)*...*(1+rn)
    total_return_factor = (1 + clean_returns).prod()

    # Determine the number of years
    # This is an approximation assuming daily data and 252 trading days per year.
    # For more accuracy, use actual start and end dates if available.
    num_periods = len(clean_returns)
    if num_periods == 0: return 0.0
    
    # Simple approximation for years
    years = num_periods / 252.0
    if years == 0:
        # If less than a year's data, CAGR might not be meaningful or could be annualized from shorter period
        # For simplicity, if less than one period, return 0 or handle as per specific requirement
        return (total_return_factor - 1) # Total return if less than a year, not annualized

    cagr = total_return_factor**(1/years) - 1
    return cagr


def Sortino(returns, required_return=0.0):
    """Calculates the Sortino Ratio (uses downside deviation)."""
    mean_ret = np.nanmean(returns)
    downside_returns = returns[returns < required_return].copy() # Use .copy() to avoid SettingWithCopyWarning
    downside_returns_std = np.nanstd(downside_returns)

    if pd.isna(downside_returns_std) or downside_returns_std == 0:
        # If no downside deviation (e.g., all returns >= required_return, or only one downside return)
        if mean_ret > required_return:
            return np.inf # Positive excess return with no downside risk
        else:
            return 0.0 # No excess return, or undefined
    
    return (mean_ret - required_return) / downside_returns_std


def MaxReturnToVol(returns):
    """Calculates the ratio of the maximum single period return to standard deviation."""
    max_ret = np.nanmax(returns)
    std_dev = np.nanstd(returns)
    if std_dev == 0: return np.inf if max_ret > 0 else (-np.inf if max_ret < 0 else 0.0)
    return max_ret / std_dev


def Skewness(returns):
    """Calculates the skewness of the returns distribution."""
    return stats.skew(returns.dropna())


def Kurtosis(returns):
    """Calculates the Fisher (excess) kurtosis of the returns distribution."""
    return stats.kurtosis(returns.dropna(), fisher=True)


def evaluate_risk_metrics(returns_series: pd.Series, alpha=0.05, horizon=1):
    """
    Evaluates a suite of risk and performance metrics for a given returns series.

    Args:
        returns_series (pd.Series): A pandas Series of arithmetic returns, indexed by date/time.
                                    Log returns should be converted to arithmetic if CAGR/MDD expect that.
                                    This version assumes arithmetic for CAGR and MDD.
        alpha (float): The significance level for VaR and CVaR (e.g., 0.05 for 95% confidence).
        horizon (int): The time horizon (in periods matching returns) for VaR/CVaR scaling.

    Returns:
        dict: A dictionary containing the calculated metrics.
    """
    if not isinstance(returns_series, pd.Series):
        returns_series = pd.Series(returns_series)
    
    # For VaR, CVaR, Skew, Kurtosis, MeanOverStd, Sortino, MaxReturnToVol, log returns are often preferred.
    # However, your original calculate_log_returns is separate.
    # If the input `returns_series` is already log returns, then CAGR and MaxDrawdown need adjustment
    # or the input should be arithmetic returns.
    # Assuming `returns_series` are arithmetic for this version of evaluate_risk_metrics.
    
    # Using original returns_series for MaxDrawdown as it expects arithmetic returns and uses its index
    # NaNs should be handled within MaxDrawdown if necessary (e.g., by fillna(0) for cumprod)
    drawdown, drawdown_date = MaxDrawdown(returns_series)

    # For most statistical measures, drop NaNs from the returns
    returns_clean = returns_series.dropna()
    if returns_clean.empty:
        print("[Warning] Returns series is empty after dropping NaNs. Cannot calculate most metrics.")
        # Still return drawdown if it was calculable from the original series
        return {"MaxDrawdown": drawdown, "MaxDrawdownDate": drawdown_date}

    cagr_period = CAGR(returns_clean) # CAGR function assumes arithmetic returns
    parametric_var = ParametricVaR(returns_clean, alpha, horizon)
    calmar_ratio = cagr_period / drawdown if drawdown != 0 and not pd.isna(drawdown) else np.nan

    results = {
        f"ParametricVaR_alpha{alpha}": parametric_var,
        f"HistoricalVaR_alpha{alpha}": HistoricalVaR(returns_clean, alpha, horizon),
        f"ParametricCVaR_alpha{alpha}": ParametricCVaR(returns_clean, alpha, horizon),
        f"HistoricalCVaR_alpha{alpha}": HistoricalCVaR(returns_clean, alpha, horizon),
        "MaxDrawdown": drawdown,
        "MaxDrawdownDate": drawdown_date,
        "MeanOverStd (Daily)": MeanOverStd(returns_clean), # Assuming daily returns if not specified
        "CAGR": cagr_period,
        "Calmar Ratio": calmar_ratio,
        "Sortino Ratio": Sortino(returns_clean),
        "MaxReturnToVol Ratio": MaxReturnToVol(returns_clean),
        "Skewness": Skewness(returns_clean),
        "Kurtosis": Kurtosis(returns_clean)
    }
    # Add number of observations
    results["Observations"] = len(returns_clean)
    return results
