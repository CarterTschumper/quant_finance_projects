import math
import numpy as np
import matplotlib.pyplot as plt
class GeometricBrownianMotionSimulator:
    def price_option(self, S0, K, r, T, sigma, num_trials=10000, num_steps=252, option_type="call", plot_avg_path=False):
        """
        Monte Carlo simulation to price a European option using GBM, with optional average path plotting.

        Parameters:
        - S0: initial stock price
        - K: strike price
        - r: risk-free rate
        - T: time to maturity (in years)
        - sigma: volatility
        - num_trials: number of simulated paths
        - num_steps: steps per path (default: 252 for daily)
        - option_type: "call" or "put"
        - plot_avg_path: whether to plot the average price path over time
        """
        if option_type not in ("call", "put"):
            raise ValueError("option_type must be either 'call' or 'put'")

        dt = T / num_steps
        discount_factor = math.exp(-r * T)
        payoffs = []
        all_paths = np.zeros((num_trials, num_steps + 1))

        for trial in range(num_trials):
            prices = [S0]
            for _ in range(num_steps):
                Z = np.random.normal()
                S_t = prices[-1] * math.exp((r - 0.5 * sigma**2) * dt + sigma * math.sqrt(dt) * Z)
                prices.append(S_t)
            all_paths[trial] = prices

            final_price = prices[-1]
            if option_type == "call":
                payoffs.append(max(final_price - K, 0))
            else:
                payoffs.append(max(K - final_price, 0))

        option_price = discount_factor * np.mean(payoffs)

        if plot_avg_path:
            avg_path = np.mean(all_paths, axis=0)
            time_points = np.linspace(0, T, num_steps + 1)
            plt.plot(time_points, avg_path, label="Average Price Path", color="blue")
            plt.title("Average GBM Stock Price Path")
            plt.xlabel("Time (Years)")
            plt.ylabel("Stock Price")
            plt.grid(True)
            plt.legend()
            plt.show()

        return option_price