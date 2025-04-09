import math
from scipy.stats import norm
class BlackScholes:
    def price(self, s, k, r, t, sigma, option_type="call"):
        """Black-Scholes call price calculation"""
        d1 = (math.log(s/k) + (r + (sigma**2/2)*t))/(sigma*math.sqrt(t))
        d2 = d1 - sigma*math.sqrt(t)

        if option_type not in ("call", "put"):
            raise ValueError("option_type must be either 'call' or 'put'")
        
        if option_type == "call":
            return s * norm.cdf(d1) - k * math.exp(-r*t) * norm.cdf(d2)
        else:
            return k * math.exp(-r*t) * norm.cdf(-d2) - s * norm.cdf(-d1)
