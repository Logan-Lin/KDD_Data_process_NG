import sys

sys.path.append("../")

import pandas as pd
import numpy as np
import h5py
from datetime import datetime, timedelta

from utils import ld_raw_fetch, bj_raw_fetch

format_string = "%Y-%m-%d %H:%M:%S"


# Fetch timestamp from h5 file and loaded them into an numpy array.
def fetch_timestamps(aq_name, file_directory):
    h5_file = h5py.File(file_directory, "r")
    raw_data = h5_file[aq_name]
    timestamp_matrix = raw_data[:, :, 0]
    return timestamp_matrix


def get_month_data(aq_name, dt_string, shortest=300):
    end_dt = datetime.strptime(dt_string, format_string)
    start_dt = end_dt - timedelta(days=30)
    selected_df = df_dict[aq_name].loc[start_dt:end_dt, ["PM2.5", "PM10"]]
    if selected_df.size < shortest:
        raise KeyError
    avg = list(selected_df.mean(axis=0))
    max = list(selected_df.max(axis=0))
    min = list(selected_df.min(axis=0))
    variance = list(selected_df.var(axis=0))
    return [avg, max, min, variance]


df_dict = dict()
for aq_name in ld_raw_fetch.aq_location:
    df = pd.read_csv("../data_ld_m/aq/{}.csv".format(aq_name), names=["utctime", "PM2.5", "PM10", "NO2"])
    df = df.dropna(axis=0, how='any')
    date_index = pd.to_datetime(df["utctime"])
    df = df.drop(["utctime"], axis=1)
    df = df.set_index([date_index])
    df_dict[aq_name] = df


if __name__ == "__main__":
    statistic = list(get_month_data("BL0", "2017-12-01 00:00:00"))
    print()
