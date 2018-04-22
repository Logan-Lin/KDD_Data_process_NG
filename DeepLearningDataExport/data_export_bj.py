import math
import os
from datetime import datetime, timedelta
import time as ti

import h5py
import numpy as np
from matplotlib import pyplot as plt

from utils.bj_raw_fetch import load_all
from utils.tools import per_delta


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
def coor_to_id(grid_coor):
    for index in range(len(grid_location.values())):
        if math.isclose(grid_coor[0], list(grid_location.values())[index][0], abs_tol=0.005) and \
                math.isclose(grid_coor[1], list(grid_location.values())[index][1], abs_tol=0.005):
            return list(grid_location.keys())[index]


def check_valid(aq_name, start_object, span):
    try:
        aq_dict = aq_dicts[aq_name]
        aq_matrix = []
        near_grid_data = []
        for i in range(span - 1, -1, -1):
            valid_dt_string = (start_object - timedelta(hours=i)).strftime(format_string)
            aq_matrix.append(aq_dict[valid_dt_string])

            near_grid_data_onehour = []
            for row in sequence:
                grid_in_row = []
                for column in row:
                    grid_in_row.append(grid_dicts[near_grids[column]][valid_dt_string])
                near_grid_data_onehour.append(grid_in_row)
            near_grid_data.append(near_grid_data_onehour)
        predict_matrix = []
        for i in range(1, predict_span + 1):
            predict_matrix.append([aq_dict[(start_object + timedelta(hours=i)).strftime(format_string)]
                                   [column] for column in [0, 1, 4]])
    except KeyError:
        return None, None, None
    return aq_matrix, predict_matrix, near_grid_data


def get_grids(aq_name, n):
    aq_coor = list(map(float, aq_location[aq_name]))
    center_id, center_coor = get_nearest(aq_coor)
    grid_coors = [center_coor]
    n = int((n - 1) / 2)
    for i in range(1, n + 1):
        for a in [1, -1]:
            grid_coors += [[center_coor[0] + a * 0.2 * i, center_coor[1]],
                           [center_coor[0], center_coor[1] + a * 0.2 * i]]
            for j in range(1, 2 * i):
                grid_coors += [[center_coor[0] + a * 0.2 * i - a * 0.1 * j,
                                center_coor[1] + a * 0.1 * j],
                               [center_coor[0] - a * 0.1 * j,
                                center_coor[1] - a * 0.1 * j + a * 0.2 * i]]
    return [coor_to_id(grid_coor) for grid_coor in grid_coors], np.array(grid_coors)


aq_location, grid_location, aq_dicts, grid_dicts = load_all()
format_string = "%Y-%m-%d %H:%M:%S"
date_format_string = "%Y_%m_%d"
start_datetime, end_datetime = datetime.strptime("2017-01-01 00:00:00", format_string), \
                               datetime.strptime("2018-01-10 00:00:00", format_string)
time_span = 24
predict_span = 50

grid_circ = 7
sequence = [[37, 39, 41, 43, 45, 47, 38],
            [36, 17, 19, 21, 23, 18, 40],
            [34, 16, 5, 7, 6, 20, 42],
            [32, 14, 4, 0, 8, 33, 44],
            [30, 12, 2, 3, 1, 24, 46],
            [28, 10, 15, 13, 11, 9, 48],
            [26, 35, 33, 31, 29, 27, 25]]
# sequence = [[5, 7, 6], [4, 0, 8], [2, 3, 1]]
data_dir = "../data/h5"
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
aq_count = 0
for aq_name in aq_location.keys():
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
    for dt_object in per_delta(start_datetime, end_datetime, timedelta(hours=1)):
        dt_string = dt_object.strftime(format_string)

        # Fetch history and prediction data, check data validation in the same time
        aq_matrix, predict, near_grid_data = check_valid(aq_name, dt_object, time_span)
        if aq_matrix is None:
            continue

        # Append this hour's data into per-day data
        grid_matrix.append(near_grid_data)
        history_matrix.append(aq_matrix)
        predict_matrix.append(predict)
        dt_int_array.append(int(ti.mktime(dt_object.timetuple())))
        valid_count += 1

    h5_file = h5py.File("".join([data_dir, "/", aq_name, ".h5"]), "w")
    h5_file.create_dataset("grid", data=np.asarray(grid_matrix))
    h5_file.create_dataset("history", data=np.asarray(history_matrix))
    h5_file.create_dataset("predict", data=np.asarray(predict_matrix))
    h5_file.create_dataset("timestep", data=np.asarray(dt_int_array))
    h5_file.flush()
    h5_file.close()
    aq_count += 1
    print("Valid data count in", aq_name, valid_count, end=', ')
    print("data exported %3.f%%" % (100 * aq_count / len(aq_location.keys())))
