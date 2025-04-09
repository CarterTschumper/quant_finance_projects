import math
import numpy as np
sqN = math.sqrt(.2)
st = [100]
for i in range(20):
    wt = sqN*np.random.normal()
    stt = st[i-1]*math.exp((.05-.5*.2**2)*.2 + .2*wt)
    st.append(stt)

print(st)