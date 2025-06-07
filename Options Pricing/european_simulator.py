import numpy as np
import matplotlib.pyplot as plt

class EuropeanOptionSimulator:
    def __init__(self, S0, K, T, r, sigma, option_type="call"):
        if option_type not in ("call", "put"):
            raise ValueError("option_type must be either 'call' or 'put'")
        
        self.S0 = S0
        self.K = K
        self.T = T
        self.r = r
        self.sigma = sigma
        self.option_type = option_type

    def payoff(self, ST):
        if self.option_type == "call":
            return np.maximum(ST - self.K, 0)
        else:
            return np.maximum(self.K - ST, 0)

    def monte_carlo_price(self, n_simulations=100000, n_steps=252, seed=None, plot_avg_path=False):
        if seed is not None:
            np.random.seed(seed)

        dt = self.T / n_steps
        Z = np.random.normal(size=(n_simulations, n_steps))
        paths = np.zeros((n_simulations, n_steps + 1))
        paths[:, 0] = self.S0

        for t in range(1, n_steps + 1):
            drift = (self.r - 0.5 * self.sigma ** 2) * dt
            diffusion = self.sigma * np.sqrt(dt) * Z[:, t - 1]
            paths[:, t] = paths[:, t - 1] * np.exp(drift + diffusion)

        ST = paths[:, -1]
        payoffs = self.payoff(ST)
        price = np.exp(-self.r * self.T) * np.mean(payoffs)

        if plot_avg_path:
            time_grid = np.linspace(0, self.T, n_steps + 1)
            avg_path = np.mean(paths, axis=0)
            plt.plot(time_grid, avg_path, label="Average Path", color="blue")

            # Plot a few sample paths
            for i in range(10):
                plt.plot(time_grid, paths[i], alpha=0.3, linewidth=0.8, color="gray")

            plt.title("Sample GBM Paths with Average")
            plt.xlabel("Time (Years)")
            plt.ylabel("Stock Price")
            plt.grid(True)
            plt.legend()
            plt.show()

        return price
