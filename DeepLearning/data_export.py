from deepkdd import raw_fetch
import math
from datetime import datetime, timedelta
from deepkdd.tools import per_delta
import h5py
import numpy as np
import os


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
        if math.isclose(grid_coor[0], list(grid_location.values())[index][0], abs_tol=0.1) and \
                math.isclose(grid_coor[1], list(grid_location.values())[index][1]):
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
        predict_row = aq_dict[(start_object + timedelta(hours=1)).strftime(format_string)]
    except KeyError:
        return None, None, None
    predict = [predict_row[i] for i in [0, 1, 4]]
    return aq_matrix, predict, near_grid_data


def get_grids(aq_name, n):
    aq_coor = list(map(float, aq_location[aq_name]))
    center_id, center_coor = get_nearest(aq_coor)
    grid_coors = [center_coor]
    n = int((n + 1) / 2)
    for i in range(1, n):
        for a in [1, -1]:
            grid_coors += [[center_coor[0] + a * 0.2 * i, center_coor[1]],
                           [center_coor[0], center_coor[1] + a * 0.2 * i]]
            for j in range(1, i + 1):
                grid_coors += [[center_coor[0] + a * 0.2 * i - a * 0.1 * j,
                                center_coor[1] + a * 0.1 * j],
                               [center_coor[0] - a * 0.1 * j,
                                center_coor[1] - a * 0.1 * j + a * 0.2 * i]]
    return [coor_to_id(grid_coor) for grid_coor in grid_coors]


aq_location, grid_location, aq_dicts, grid_dicts = raw_fetch.load_all()
format_string = "%Y-%m-%d %H:%M:%S"
date_format_string = "%Y_%m_%d"
start_datetime, end_datetime = datetime.strptime("2017-01-01 00:00:00", format_string), \
                               datetime.strptime("2018-01-10 00:00:00", format_string)
time_span = 3
grid_circ = 3
sequence = [[5, 7, 6], [4, 0, 8], [2, 3, 1]]
data_dir = "../data/h5"
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
aq_count = 0
for aq_name in aq_location.keys():
    directory = "".join([data_dir, "/", aq_name])
    if not os.path.exists(directory):
        os.makedirs(directory)
    near_grids = get_grids(aq_name, grid_circ)

    # Exporting data from start to end (per day)
    for d_object in per_delta(start_datetime, end_datetime, timedelta(days=1)):
        grid_matrix = []
        history_matrix = []
        predict_matrix = []

        # Exporting data in one day
        valid = False
        for dt_object in per_delta(d_object, d_object + timedelta(hours=24), timedelta(hours=1)):
            dt_string = dt_object.strftime(format_string)

            # Fetch history and prediction data, check data validation in the same time
            aq_matrix, predict, near_grid_data = check_valid(aq_name, dt_object, 3)
            if aq_matrix is None:
                continue
            valid = True

            # Append this hour's data into per-day data
            grid_matrix.append(near_grid_data)
            history_matrix.append(aq_matrix)
            predict_matrix.append(predict)
        if valid:
            date_string = d_object.date().strftime(date_format_string)
            h5_file = h5py.File("".join([directory, "/",
                                         date_string, ".h5"]), "w")
            h5_file.create_dataset("grid", data=np.asarray(grid_matrix))
            h5_file.create_dataset("history", data=np.asarray(history_matrix))
            h5_file.create_dataset("predict", data=np.asarray(predict_matrix))
            h5_file.flush()
            h5_file.close()
    aq_count += 1
    print("Deep learning data exported %3.f%%" % (100 * aq_count / len(aq_location.keys())))
