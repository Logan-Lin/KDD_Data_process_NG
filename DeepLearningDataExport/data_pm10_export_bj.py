import math
import os
from datetime import datetime, timedelta
from progressbar import ProgressBar as PB, Bar, Percentage
import time as ti

import h5py
import numpy as np
from matplotlib import pyplot as plt

from utils.bj_raw_fetch import load_all, load_aq_original, load_aq_pm10_dicts
from utils.tools import per_delta, angle_to_int, get_one_hot


# Get the nearest grid id and coordination close to the given coordinate
def get_nearest(coor):
    min_distance = 999999
    min_grid = ""
    min_coor = []
    for key in grid_location.keys():
        grid_coor = grid_location[key]
        dist = math.sqrt(math.pow(grid_coor[0] - coor[0], 2) +
                         (math.pow(grid_coor[1] - coor[1], 2)))
        if min_distance > dist:
            min_distance = dist
            min_grid = key
            min_coor = grid_coor
    return min_grid, min_coor


# Turn coordinate to grid id
def coordinate_to_id(grid_coordinate):
    for index in range(len(grid_location.values())):
        if math.isclose(grid_coordinate[0], list(grid_location.values())[index][0], abs_tol=0.005) and \
                math.isclose(grid_coordinate[1], list(grid_location.values())[index][1], abs_tol=0.005):
            return list(grid_location.keys())[index]


def check_valid(aq_name, start_object, span):
    try:
        aq_dict = aq_dicts[aq_name]
        aq_dict_no_pm10 = aq_dicts_no_pm10[aq_name]
        aq_matrix = []
        near_grid_data = []
        fake_forecast_data = []
        for i in range(span - 1, -1, -1):
            valid_dt_string = (start_object - timedelta(hours=i)).strftime(format_string)
            aq_matrix.append(aq_dict_no_pm10[valid_dt_string])

            near_grid_data_onehour = []
            for row in near_grids:
                grid_in_row = []
                for column in row:
                    grid_in_row.append(grid_dicts[column][valid_dt_string])
                near_grid_data_onehour.append(grid_in_row)
            near_grid_data.append(near_grid_data_onehour)
        predict_matrix = []
        for i in range(1, predict_span + 1):
            predict_matrix.append([aq_dict[(start_object + timedelta(hours=i)).strftime(format_string)]
                                   [column] for column in [0, 1, 4]])
            fake_forecast_data.append(get_fake_forecast_data(
                aq_name, (start_object + timedelta(hours=i)).strftime(format_string)))
    except KeyError:
        return None, None, None, None
    return aq_matrix, predict_matrix, near_grid_data, fake_forecast_data


def get_grids(aq_name, n):
    aq_coordinate = aq_location[aq_name]
    center_id, center_coordinate = get_nearest(aq_coordinate)
    grid_coor_matrix = []
    grid_coor_nparray = []
    n = int((n - 1) / 2)
    for i in range(-n, n + 1):
        grid_row = []
        for a in range(n, -n - 1, -1):
            grid_row.append(coordinate_to_id([center_coordinate[0] - i * 0.1, center_coordinate[1] - a * 0.1]))
            grid_coor_nparray.append([center_coordinate[0] - i * 0.1, center_coordinate[1] - a * 0.1])
        grid_coor_matrix.append(grid_row)
    return grid_coor_matrix, np.asarray(grid_coor_nparray)


def get_fake_forecast_data(aq_name, dt_string):
    grid_id, grid_coor = get_nearest(aq_location[aq_name])
    data_row = grid_dicts[grid_id][dt_string]
    # Temperature, humidity, wind direction, wind speed
    return [data_row[0], data_row[2], data_row[4]] + get_one_hot(angle_to_int(data_row[3]), 16)


aq_location, grid_location, aq_dicts, grid_dicts = load_all()
aq_dicts_no_pm10 = load_aq_pm10_dicts()
# aq_dicts_no_pm10 = aq_dicts
format_string = "%Y-%m-%d %H:%M:%S"
date_format_string = "%Y_%m_%d"
start_datetime, end_datetime = datetime.strptime("2017-01-01 00:00:00", format_string), \
                               datetime.strptime("2018-04-22 00:00:00", format_string)
diff = end_datetime - start_datetime
days, seconds = diff.days, diff.seconds
delta_time = int(days * 24 + seconds // 3600)
time_span = 24
predict_span = 50

grid_circ = 7
data_dir = "../data/h5_pm10"

if __name__ == '__main__':
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    aq_count = 0
    for aq_name in aq_location.keys():
        aggregate = 0

        ti.sleep(0.1)
        bar = PB(initial_value=0, maxval=delta_time + 1,
                 widgets=[aq_name, ' ', Bar('=', '[', ']'), ' ', Percentage()])

        valid_count = 0
        near_grids, grid_coor_array = get_grids(aq_name, grid_circ)

        # Validate the near grid matrix algorithm
        # plt.figure()
        # plt.title(aq_name)
        # plt.plot(aq_location[aq_name][0], aq_location[aq_name][1], '.')
        # plt.plot(grid_coor_array[:, 0], grid_coor_array[:, 1], '.')
        # plt.show()

        # Exporting data from start to end
        grid_matrix = []
        history_matrix = []
        predict_matrix = []
        dt_int_array = []
        fake_forecast_matrix = []
        for dt_object in per_delta(start_datetime, end_datetime, timedelta(hours=1)):
            aggregate += 1
            bar.update(aggregate)
            dt_string = dt_object.strftime(format_string)

            # Fetch history and prediction data, check data validation in the same time
            aq_matrix, predict, near_grid_data, fake_forecast_data = check_valid(aq_name, dt_object, time_span)
            if aq_matrix is None:
                continue

            # Append this hour's data into per-day data
            grid_matrix.append(near_grid_data)
            history_matrix.append(aq_matrix)
            predict_matrix.append(predict)
            dt_int_array.append(int(ti.mktime(dt_object.timetuple())))
            fake_forecast_matrix.append(fake_forecast_data)
            valid_count += 1

        h5_file = h5py.File("".join([data_dir, "/", aq_name, ".h5"]), "w")
        h5_file.create_dataset("grid", data=np.asarray(grid_matrix))
        h5_file.create_dataset("history", data=np.asarray(history_matrix))
        h5_file.create_dataset("predict", data=np.asarray(predict_matrix))
        h5_file.create_dataset("timestep", data=np.asarray(dt_int_array))
        h5_file.create_dataset("weather_forecast", data=np.asarray(fake_forecast_matrix))
        h5_file.flush()
        h5_file.close()
        aq_count += 1
        print(" - valid%6.2f%%(%5d)" % ((100 * valid_count / aggregate), valid_count))
