from black_scholes import BlackScholes
from european_simulator import EuropeanOptionSimulator

bs = BlackScholes()
price = bs.price(s=100, k=110, r=0.05, t=1, sigma=0.1, option_type="call")
print(price)

opt = EuropeanOptionSimulator(S0=100, K=110, T=1, r=0.05, sigma=0.1, option_type="call")
price = opt.monte_carlo_price(n_simulations=100000, seed=42, plot_avg_path=True)
print(f"Simulated Option Price: {price:.4f}")