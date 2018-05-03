import h5py
import numpy as np
from utils import ld_raw_fetch, bj_raw_fetch
from matplotlib import pyplot as plt
from datetime import datetime as dt


format_string = "%Y-%m-%d %H:%M:%S"

start_string, end_string = "2018-04-01-00", "2018-04-29-22"
# aq_dicts = ld_raw_fetch.load_aq_dicts_none(start_string, end_string)
aq_dicts = ld_raw_fetch.load_aq_history_dicts()
aq_dict = aq_dicts["BL0"]
aq_array = np.array(list(aq_dict.values()))
x = range(len(aq_dict.keys()))
for i in range(2):
    plt.plot(x, aq_array[:, i])
plt.show()
