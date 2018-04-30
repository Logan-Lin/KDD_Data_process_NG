import os
from datetime import datetime, timedelta

import h5py
import numpy as np

from forecast import parse
from utils.ld_raw_fetch import *
from utils.tools import per_delta, angle_to_int, get_one_hot


# Get the nearest grid id and coordination close to the given coordinate
def get_nearest(coordinate):
    min_distance = 999999
    min_grid = ""
    min_coordinate = []
    for key in grid_location.keys():
        grid_coor = grid_location[key]
        dist = math.sqrt(math.pow(grid_coor[0] - coordinate[0], 2) +
                         (math.pow(grid_coor[1] - coordinate[1], 2)))
        if min_distance > dist:
            min_distance = dist
            min_grid = key
            min_coordinate = grid_coor
    return min_grid, min_coordinate


# Turn coordinate to grid id
def coordinate_to_id(grid_coordinate):
    for index in range(len(grid_location.values())):
        if math.isclose(grid_coordinate[0], list(grid_location.values())[index][0], abs_tol=0.005) and \
                math.isclose(grid_coordinate[1], list(grid_location.values())[index][1], abs_tol=0.005):
            return list(grid_location.keys())[index]


def check_valid(aq_name, start_object, span):
    try:
        aq_dict = aq_dicts[aq_name]
        aq_matrix = []
        near_grid_data = []
        fake_forecast_data = []
        need_fake = False
        try:
            fake_forecast_data = get_forecast_data(start_object)
        except FileNotFoundError:
            need_fake = True
        for i in range(span - 1, -1, -1):
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
        predict_matrix = []
        if need_fake:
            for i in range(1, predict_span + 1):
                fake_forecast_data.append(get_fake_forecast_data(
                    aq_name, (start_object + timedelta(hours=i)).strftime(format_string)))
    except KeyError as e:
        return None, None, None, None
    return aq_matrix, predict_matrix, near_grid_data, fake_forecast_data


def get_fake_forecast_data(aq_name, dt_string):
    grid_id, grid_coor = get_nearest(aq_location[aq_name])
    data_row = grid_dicts[grid_id][dt_string]
    # Temperature, humidity, wind direction, wind speed
    return [data_row[0], data_row[2], data_row[4]] + get_one_hot(angle_to_int(data_row[3]), 16)


def get_forecast_data(dt_object):
    file_directory = "../forecast/data/ld_{}.txt".format((dt_object + timedelta(hours=8)).strftime("%m_%d_%H"))
    return parse.get_data(file_directory)


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


if __name__ == '__main__':
    start_string, end_string = "2018-04-01-00", "2018-04-29-22"
    aq_location, grid_location, aq_dicts, grid_dicts = load_all(start_string, end_string)
    # aq_dicts = load_filled_dicts(start_string, end_string)
    format_string = "%Y-%m-%d %H:%M:%S"
    format_string_2 = "%Y-%m-%d-%H"

    start_string, end_string = "2018-04-01-22", "2018-04-29-22"
    start_datetime, end_datetime = datetime.strptime(start_string, format_string_2), \
                                   datetime.strptime(end_string, format_string_2)
    diff = end_datetime - start_datetime
    days, seconds = diff.days, diff.seconds
    delta_time = int(days * 24 + seconds // 3600)

    time_span = 24
    grid_edge_length = 7
    predict_span = 50
    data_dir = "../data_ld/h5_history/{}_{}".format(start_string, end_string)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    aq_count = 0
    print("\nFetching data to export...")
    for aq_name in aq_location.keys():
        # if not aq_name == "KF1":
        #     continue

        aggregate = 0

        valid_count = 0
        near_grids, grid_coor_array = get_grids(aq_name, grid_edge_length)

        # Validate the near grid matrix algorithm
        # plt.figure()
        # plt.title(aq_name)
        # plt.plot(aq_location[aq_name][0], aq_location[aq_name][1], '.')
        # plt.plot(grid_coor_array[:, 0], grid_coor_array[:, 1], '.')
        # plt.show()

        last_valid_dt = None
        for dt_object_day in per_delta(start_datetime, end_datetime, timedelta(hours=24)):
            for dt_object in per_delta(dt_object_day, dt_object_day + timedelta(hours=0), timedelta(hours=1)):
                aggregate += 1
                dt_string = dt_object.strftime(format_string)

                # Fetch history and prediction data, check data validation in the same time
                aq_matrix, predict, near_grid_data, fake_forecast_data = check_valid(aq_name, dt_object, time_span)
                if aq_matrix is None:
                    continue

                grid_matrix = [near_grid_data]
                history_matrix = [aq_matrix]
                # predict_matrix.append(predict)
                dt_int_array = [dt_object.timestamp()]
                fake_forecast_matrix = [fake_forecast_data]
                valid_count += 1

                last_valid_dt = dt_object

        if last_valid_dt is not None:
            h5_file = h5py.File("{}/{}.h5".format(data_dir, aq_name), "w")
            h5_file.create_dataset("grid", data=np.asarray(grid_matrix))
            h5_file.create_dataset("history", data=np.asarray(history_matrix))
            # h5_file.create_dataset("predict", data=np.asarray(predict_matrix))
            h5_file.create_dataset("timestep", data=np.asarray(dt_int_array))
            h5_file.create_dataset("weather_forecast", data=np.asarray(fake_forecast_matrix))
            h5_file.flush()
            h5_file.close()
            print("{} - last valid {}".format(aq_name, last_valid_dt.strftime(format_string_2)))
        else:
            print("{} - No valid".format(aq_name))
