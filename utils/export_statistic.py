import sys
sys.path.append("../")

import h5py
from datetime import datetime, timedelta

format_string = "%Y-%m-%d %H:%M:%S"


# Fetch timestamp from h5 file and loaded them into an numpy array.
def fetch_timestamps(aq_name, file_directory):
    h5_file = h5py.File(file_directory, "r")
    raw_data = h5_file[aq_name]
    timestamp_matrix = raw_data[:, :, 0]
    return timestamp_matrix


def get_month_data(aq_name, dt_string, df, shortest=300):
    end_dt = datetime.strptime(dt_string, format_string)
    start_dt = end_dt - timedelta(days=30)
    selected_df = df.loc[aq_name, :]
    selected_df = selected_df.loc[start_dt:end_dt, ["pm2.5", "pm10"]]
    if selected_df.size < shortest:
        raise KeyError
    avg = list(selected_df.mean(axis=0))
    max = list(selected_df.max(axis=0))
    min = list(selected_df.min(axis=0))
    variance = list(selected_df.var(axis=0))
    return [avg, max, min, variance]
