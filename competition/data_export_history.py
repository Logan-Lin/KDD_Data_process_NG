import sys
sys.path.append("../")

import os
from datetime import datetime, timedelta
from matplotlib import pyplot as plt

import h5py
import numpy as np
import argparse

from forecast import parse
from utils import bj_raw_fetch, ld_raw_fetch
from utils.tools import *


format_string = "%Y-%m-%d %H:%M:%S"
format_string_2 = "%Y-%m-%d-%H"
time_span = 24
predict_span = 50
grid_circ = 7

forecast_directory_dict = {"ld": "../forecast/data/bj_{}.txt",
                     "bj": "../forecast/data/ld_{}.txt"}
export_directory_dict = {"bj": "../data/h5_history/{}_{}",
                    "ld": "../data_ld/h5_history/{}_{}"}
forecast_directory = ""
export_directory = ""

aq_dicts, grid_dicts, grid_location, aq_location = dict(), dict(), dict(), dict()


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


def check_valid(aq_name, start_object, near_grids):
    try:
        aq_dict = aq_dicts[aq_name]
        aq_matrix = []
        near_grid_data = []

        need_fake = False
        fake_forecast_data = []
        try:
            fake_forecast_data = get_forecast_data(start_object)
        except FileNotFoundError:
            need_fake = True
        predict_data = []
        for i in range(time_span - 1, -1, -1):
            valid_dt_string = (start_object - timedelta(hours=i)).strftime(format_string)
            aq_matrix.append(aq_dict[valid_dt_string])

            near_grid_data_onehour = []
            for row in near_grids:
                grid_in_row = []
                for column in row:
                    try:
                        grid_in_row.append(grid_dicts[column][valid_dt_string])
                    except KeyError:
                        print("Grid loss at {}".format(valid_dt_string))
                        raise KeyError
                near_grid_data_onehour.append(grid_in_row)
            near_grid_data.append(near_grid_data_onehour)
        if need_fake:
            for i in range(1, predict_span + 1):
                fake_forecast_data.append(
                    get_fake_forecast_data(
                        aq_name, (start_object + timedelta(hours=i)).strftime(format_string)))
    except KeyError:
        return None, None, None, None
    return aq_matrix, near_grid_data, fake_forecast_data, predict_data


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


def get_forecast_data(dt_object):
    file_directory = forecast_directory.format((dt_object + timedelta(hours=8)).strftime("%m_%d_%H"))
    return parse.get_data(file_directory)


def export_data(city, read_start_string, read_end_string, export_start_string=None, export_end_string=None):
    start_string, end_string = read_start_string, read_end_string
    global aq_location, grid_location, grid_dicts, aq_dicts, forecast_directory, export_directory
    forecast_directory = forecast_directory_dict[city]
    export_directory = export_directory_dict[city]
    if city == "bj":
        aq_location, grid_location, aq_dicts_, grid_dicts = bj_raw_fetch.load_all(start_string, end_string)
        aq_dicts = bj_raw_fetch.load_filled_dicts(start_string, end_string)
    elif city == "ld":
        aq_location, grid_location, aq_dicts_, grid_dicts = ld_raw_fetch.load_all(start_string, end_string)
        aq_dicts = ld_raw_fetch.load_filled_dicts(start_string, end_string)

    if export_start_string is None:
        start_string, end_string = read_start_string, read_end_string
    else:
        start_string, end_string = export_start_string, export_end_string
    start_datetime, end_datetime = datetime.strptime(start_string, format_string_2), \
                                   datetime.strptime(end_string, format_string_2)

    data_dir = export_directory.format(start_string, end_string)

    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    print("\nExporting to {}".format(data_dir))

    for aq_name in aq_location.keys():
        # if not aq_name == "fangshan_aq":
        #     continue

        valid_count = 0
        near_grids, grid_coor_array = get_grids(aq_name, grid_circ)

        # Validate the near grid matrix algorithm
        # plt.figure()
        # plt.title(aq_name)
        # plt.plot(aq_location[aq_name][0], aq_location[aq_name][1], '.')
        # plt.plot(grid_coor_array[:, 0], grid_coor_array[:, 1], '.')
        # plt.show()

        # Exporting data from start to end
        last_valid_dt_object = None
        grid_matrix, history_matrix, dt_int_array, forecast_matrix = tuple([1] * 4)
        for dt_object in per_delta(start_datetime, end_datetime, timedelta(hours=24)):
            # Fetch history and prediction data, check data validation in the same time
            aq_matrix, near_grid_data, forecast_data, predict = check_valid(aq_name, dt_object, near_grids)
            if aq_matrix is None:
                continue

            # Append this hour's data into per-day data
            grid_matrix = [near_grid_data]
            history_matrix = [aq_matrix]
            dt_int_array = [dt_object.timestamp()]
            forecast_matrix = [forecast_data]
            valid_count += 1

            last_valid_dt_object = dt_object

        if last_valid_dt_object is not None:
            h5_file = h5py.File("{}/{}.h5".format(data_dir, aq_name), "w")
            h5_file.create_dataset("grid", data=np.asarray(grid_matrix))
            h5_file.create_dataset("history", data=np.asarray(history_matrix))
            h5_file.create_dataset("timestep", data=np.asarray(dt_int_array))
            # h5_file.create_dataset("predict", data=np.asarray(predict_matrix))
            h5_file.create_dataset("weather_forecast", data=np.asarray(forecast_matrix))
            h5_file.flush()
            h5_file.close()
            print("{} - Have data, last valid {}".format(aq_name, last_valid_dt_object.strftime(format_string_2)))
        else:
            print("{} - No valid data".format(aq_name))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--city", type=str,
                        help="City, input 'bj' or 'ld' for Beijing and London", default="bj")
    parser.add_argument("-s", "--start", type=str,
                        help="Start datetime string, in YYYY-MM-DD-hh format", default="2018-04-30-22")
    parser.add_argument("-e", "--end", type=str,
                        help="End datetime string, in YYYY-MM-DD-hh format", default="2018-05-01-22")
    argv = parser.parse_args()

    export_data(argv.city, argv.start, argv.end)
