import numpy as np
import h5py
from utils import bj_raw_fetch
import os


directories = [os.path.join("..", "data", "h5_6"),
               os.path.join("..", "data", "h5_test", "s2018-04-01-00_e2018-04-29-22_h24"),
               os.path.join("..", "data", "h5_test", "s2018-04-29-22_e2018-05-21-22_h24")]

write_dir = os.path.join("h5_merge")

if not os.path.exists(write_dir):
    os.makedirs(write_dir)


for aq_name in bj_raw_fetch.aq_location.keys():
    first = True
    for directory in directories:
        directory = os.path.join(directory, aq_name + ".h5")
        h5_file = h5py.File(directory, "r")
        s_history_grid = h5_file["grid"]
        s_history_aq = h5_file["history"]
        s_predict_aq = h5_file["predict"]
        s_timestamps = h5_file["timestep"]
        s_weather_forecast = h5_file["weather_forecast"]

        if first:
            first = False
            history_grid = s_history_grid
            history_aq = s_history_aq
            predict_aq = s_predict_aq
            timestamps = s_timestamps
            weather_forecast = s_weather_forecast

            last_timestamp = s_timestamps[-1]
        else:
            try:
                start_index = np.where(abs(s_timestamps[:] - last_timestamp) < 0.1)[0][0] + 1
            except IndexError:
                start_index = 0
            history_grid = np.append(history_grid, s_history_grid[start_index:], axis=0)
            history_aq = np.append(history_aq, s_history_aq[start_index:], axis=0)
            predict_aq = np.append(predict_aq, s_predict_aq[start_index:], axis=0)
            timestamps = np.append(timestamps, s_timestamps[start_index:], axis=0)
            weather_forecast = np.append(weather_forecast, s_weather_forecast[start_index:], axis=0)

            last_timestamp = s_timestamps[-1]
        
    write_file = h5py.File(os.path.join(write_dir, aq_name + ".h5"), "w")
    write_file.create_dataset("grid", data=np.asarray(history_grid))
    write_file.create_dataset("history", data=np.asarray(history_aq))
    write_file.create_dataset("predict", data=np.asarray(predict_aq))
    write_file.create_dataset("timestep", data=np.asarray(timestamps))
    write_file.create_dataset("weather_forecast", data=np.asarray(weather_forecast))
    print("Finished merging {} data".format(aq_name))
