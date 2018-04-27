import matplotlib.pyplot as plt
import numpy as np
from utils import ld_raw_fetch, bj_raw_fetch
from datetime import datetime, timedelta
from utils import tools


grid_location = bj_raw_fetch.grid_location
coors = np.array(list(grid_location.values()))
plt.plot(coors[:, 0], coors[:, 1], '.')
plt.show()
