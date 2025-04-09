from black_scholes import BlackScholes
from GBM_pricing import GeometricBrownianMotionSimulator

bs = BlackScholes()
gbm = GeometricBrownianMotionSimulator()

price = bs.price(s=100, k=105, r=0.05, t=1, sigma=0.2, option_type="call")
print(price)

gbm_price = gbm.price_option(S0=100, K=105, r=0.05, num_steps=252, T=1, sigma=0.2, num_trials = 100000, option_type="call", plot_avg_path=True)
print(gbm_price)