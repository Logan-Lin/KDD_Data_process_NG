import csv
from datetime import datetime

import h5py
import numpy as np

# Load all needed data into dicts and lists.
# Load aq station location dict
aq_location = dict()
with open("../data/Beijing_AirQuality_Stations_cn.csv") as read_file:
    reader = csv.reader(read_file, delimiter='\t')
    for row in reader:
        try:
            aq_location[row[0]] = list(map(float, row[1:]))
        except ValueError:
            pass

aq_row = ["PM2.5", "PM10", "NO2", "CO", "O3", "SO2"]
head_row = ["aq_name", "time", "weekday", "workday", "holiday"] + aq_row + \
           ["temperature", "pressure", "humidity", "wind_direction", "wind_speed"]
h5_file = h5py.File("../data/tradition_export/traditional.h5")
all_data = [head_row]
for aq_name in aq_location.keys():
    raw_data = np.array(h5_file[aq_name])
    for row in raw_data:
        row = list(row)
        all_row = [aq_name, datetime.fromtimestamp(int(row[0]))]
        all_row += list(map(int, row[1:4]))
        all_row += row[4:]
        all_data.append(row)
