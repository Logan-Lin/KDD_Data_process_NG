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


start_str, end_str = "2017-01-01-00", "2018-04-01-00"
for aq_name in aq_location.keys():
    h5_file = h5py.File("../data/tradition_train/{}_{}/{}.h5".format(start_str, end_str, aq_name), "r")
    timestamp = h5_file["timestamp"]
    history_aq = h5_file["history_aq"]
    history_meo = h5_file["history_meo"]
    forecast = h5_file["forecast"]
    predict_aq = h5_file["predict_aq"]
    statistic = h5_file["statistic"]

