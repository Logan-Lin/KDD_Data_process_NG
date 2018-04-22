import matplotlib.pyplot as plt
import numpy as np
from utils import ld_raw_fetch
from datetime import datetime, timedelta
from utils import tools

aq_location, aq_dicts = ld_raw_fetch.load_aq()
aq_dict = aq_dicts["BL0"]
data_matrix = []

format_string = "%Y-%m-%d %H:%M:%S"
start_dt, end_dt = datetime.strptime("2017-01-01 00:00:00", format_string), \
                               datetime.strptime("2018-01-10 00:00:00", format_string)

count = 0
for dt_object in tools.per_delta(start_dt, end_dt, timedelta(hours=1)):
    count += 1
    dt_string = dt_object.strftime(format_string)
    data_matrix.append(aq_dict[dt_string])

data_matrix = np.array(data_matrix)

plt.show()
