import os
import sys
sys.path.append("../")

from competition import load_data
from datetime import datetime, timedelta

import h5py
import numpy as np
import argparse

import pandas as pd
from forecast import parse
from utils.tools import *
from utils import bj_raw_fetch, ld_raw_fetch
import math


format_string = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d-%H"]
forecast_directory_dict = {"ld": "../forecast/data/ld_{}.txt",
                     "bj": "../forecast/data/bj_{}.txt"}
export_directory_dict = {"bj": "../data/h5_history/s{}_e{}_h{}_g{}",
                    "ld": "../data_ld/h5_history/s{}_e{}_h{}_g{}"}
column_dict = {"bj": ["pm2.5", "pm10", "no2", "co", "o3", "so2"],
               "ld": ["pm2.5", "pm10"]}


def prepare_data(city, fill):
    """
    Prepare needed location, aq and grid data for the program.

    :param city: string, "bj" or "ld" for Beijing and London.
    :param fill: bool, load filled data or not.
    """
    global aq_location, grid_location, aq_df, grid_df

    # Load location data
    if city == "bj":
        aq_location, grid_location = bj_raw_fetch.load_location()
    elif city == "ld":
        aq_location, grid_location = ld_raw_fetch.load_location()

    # Load aq and meo data
    aq_load_directories = [load_data.history_data_directory[city]["aq"]]
    if fill:
        aq_load_directories.append(load_data.filled_data_directory[city])
    else:
        aq_load_directories.append(load_data.api_data_directory.format(city, "aq"))
    drop = {"bj": None, "ld": ["no2"]}[city]
    aq_df = load_data.load_directory_data(aq_load_directories, drop=drop,
                                          data_header=load_data.data_header_dict[city]["aq"])
    grid_df = load_data.load_directory_data([load_data.history_data_directory[city]["meo"],
                                             load_data.api_data_directory.format(city, "meo")],
                                            data_header=load_data.data_header_dict[city]["meo"])


def fetch_span(start, end, df, time_span, columns):
    """
    Fetch a range of data based on start and end datetime.

    :param start: datetime object, indicating the start of range.
    :param end: datetime object, indicating the end of range.
    :param df: pandas data frame, where you want to fetch data from.
    :param time_span: int, the length of data required, count in hour.
    :param columns: list, specifying the names of columns you want to include in fetched data.
    :return: fetched data, in numpy array form.
    :raise: KeyError when fetched data are no tas long as the given time_span,
        probably mean there is data loss between start and end datetime.
    """
    result = df.loc[start:end, columns]
    if result.shape[0] < time_span:
        raise KeyError("Available data not long enough")
    else:
        return result.as_matrix()


def fetch_grid(start, end, df_matrix, time_span, columns=None):
    """
    Fetch a range of grid matrix data based on given start and end datetime.

    :param start: datetime object, indicating the start of range.
    :param end: datetime object, indicating the end of range.
    :param df_matrix: n*n list matrix, each item is a pandas data frame containing corresponding grid data.
    :param time_span: int, the length of data required, count in hour.
    :param columns: list, specifying the names of columns you want to include in fetched data.
        But in most cases, you can ignore it to use the default column selection.
    :return: n*n list matrix, fetched data, each item in numpy array form.
    :raise: KeyError when fetched data are no tas long as the given time_span,
        probably mean there is data loss between start and end datetime.
    """
    if columns is None:
        columns = ["temperature", "pressure", "humidity", "wind_direction", "wind_speed"]
    grid_matrix = []
    for df_row in df_matrix:
        grid_row = []
        for df in df_row:
            result = df.loc[start:end, columns]
            if result.shape[0] < time_span:
                raise KeyError("Available grid data not long enough")
            grid_row.append(result.as_matrix())
        grid_matrix.append(grid_row)
    return grid_matrix


def get_forecast_data(dt_object, city, center_grid_df):
    """
    Fetch forecast data, either from forecast website or forged from grid meo data.

    :param dt_object: datetime object, specifying the datetime of forecast data you want to fetch.
    :param city: string, "bj" or "ld" for Beijing and London.
    :param center_grid_df: the data frame of the center of grid matrix, which is the nearest grid.
    :return: forecast data, in numpy array form.
    """
    file_directory = forecast_directory_dict[city].format((dt_object + timedelta(hours=8)).strftime("%m_%d_%H"))
    try:
        if dt_object.year == 2017:
            raise ValueError("Year 2017 are bound to lack in real forecast data")
        return np.array(parse.get_data(file_directory))
    except (FileNotFoundError, ValueError):
        return get_fake_forecast(dt_object, center_grid_df)


def get_fake_forecast(dt_object, center_grid_df, time_span=50):
    data_df = center_grid_df.loc[dt_object + timedelta(hours=1):dt_object + timedelta(hours=time_span),
                  ["temperature", "humidity", "wind_speed", "wind_direction"]]
    if data_df.shape[0] < time_span:
        raise KeyError("Available grid data for forecast not long enough")
    wind_one_hot = pd.Series(data_df["wind_direction"]).apply(lambda x: pd.Series(get_angle_one_hot(x)))
    wind_one_hot.index = data_df.index.copy()
    data_df = data_df.drop(["wind_direction"], axis=1)
    data_df = pd.concat([data_df, wind_one_hot], axis=1)
    return data_df.as_matrix()


def get_near_grid_matrix(aq_coor, grid_length):
    center_id, center_coordinate = get_nearest_grid(aq_coor)
    grid_coor_matrix = []
    grid_coor_nparray = []
    n = int((grid_length - 1) / 2)
    for i in range(-n, n + 1):
        grid_row = []
        for a in range(n, -n - 1, -1):
            grid_row.append(coordinate_to_id([center_coordinate[0] - i * 0.1, center_coordinate[1] - a * 0.1]))
            grid_coor_nparray.append([center_coordinate[0] - i * 0.1, center_coordinate[1] - a * 0.1])
        grid_coor_matrix.append(grid_row)
    return grid_coor_matrix, np.asarray(grid_coor_nparray)


def get_nearest_grid(coor):
    min_distance = 999999
    min_grid = ""
    min_coor = []
    for grid_name, grid_coor in grid_location.items():
        dist = math.sqrt(math.pow(grid_coor[0] - coor[0], 2) +
                         (math.pow(grid_coor[1] - coor[1], 2)))
        if min_distance > dist:
            min_distance = dist
            min_grid = grid_name
            min_coor = grid_coor
    return min_grid, min_coor


def coordinate_to_id(grid_coor):
    for index in range(len(grid_location.values())):
        if math.isclose(grid_coor[0], list(grid_location.values())[index][0], abs_tol=0.005) and \
                math.isclose(grid_coor[1], list(grid_location.values())[index][1], abs_tol=0.005):
            return list(grid_location.keys())[index]


def export_data(city, start, end, train, fill, history_length, predict_length, grid_length, time_delta=1):
    """
    Export data for train or prediction.

    :param city: string, "bj" or "ld" for Beijing and London.
    :param start: datetime string or list, each in "%Y-%m-%d-%H" format. Indicate the start of data exportation.
    :param end: datetime string or list, same format as start. Indicate the end of data exportation.
    :param train: bool, true to export training data, else export predict data.
    :param fill: bool, ues filled data or not.
    :param history_length: integer, indicate the time span (in hour) of history to export.
    :param predict_length: integer, indicate the time span (in hour) of prediction to export.
        When exporting data for prediction, this value will be useless.
    :param grid_length: The side length of nearby grid matrix.
    :param time_delta: The timedelta to traverse through.
    """
    prepare_data(city, fill)

    if not isinstance(start, list):
        start = [start]
    if not isinstance(end, list):
        end = [end]

    for times in range(len(start)):
        single_start = start[times]
        single_end = end[times]
        data_dir = export_directory_dict[city].format(single_start, single_end, history_length, grid_length)
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        print("Exporting to directory {}".format(data_dir))

        start_dt, end_dt = datetime.strptime(single_start, format_string[1]), \
                           datetime.strptime(single_end, format_string[1])

        diff = end_dt - start_dt
        days, seconds = diff.days, diff.seconds
        delta_time = int(days * 24 + seconds // 3600)
        delta_time = int(delta_time / time_delta)
        split_time = int(delta_time / 20)

        for aq_name, aq_coor in aq_location.items():
            aggregate_count = 0
            near_grid_names, near_grid_coors = get_near_grid_matrix(aq_coor, grid_length)
            single_aq_df = aq_df.loc[aq_name, :]

            grid_df_matrix = []
            for grid_name_row in near_grid_names:
                grid_df_row = []
                for grid_name in grid_name_row:
                    grid_df_row.append(grid_df.loc[grid_name, :])
                grid_df_matrix.append(grid_df_row)

            center_grid_df = grid_df_matrix[int((grid_length - 1) / 2)][int((grid_length - 1) / 2)]

            history_aq, history_grid, forecast, timestamps, predict_aq = [], [], [], [], []
            valid = 0
            last_valid_dt = None
            for dt in per_delta(start_dt, end_dt, timedelta(hours=time_delta)):
                aggregate_count += 1
                if aggregate_count % split_time == 0:
                    print("\t{} exported %3.2f%%".format(aq_name) % (100 * aggregate_count / delta_time))
                try:
                    history_aq.append(fetch_span(start=dt - timedelta(hours=history_length - 1), end=dt,
                                                 df=single_aq_df, time_span=history_length,
                                                 columns=column_dict[city]))
                    history_grid.append(fetch_grid(start=dt - timedelta(hours=history_length - 1), end=dt,
                                                   df_matrix=grid_df_matrix, time_span=history_length))
                    forecast.append(get_forecast_data(dt_object=dt, city=city, center_grid_df=center_grid_df))
                    timestamps.append(dt.timestamp())
                    if train:
                        predict_aq.append(fetch_span(start=dt + timedelta(hours=1), end=dt + timedelta(hours=50),
                                                     df=single_aq_df, time_span=predict_length,
                                                     columns=column_dict[city]))
                    valid += 1
                    last_valid_dt = dt
                except KeyError:
                    continue
            h5_file = h5py.File(os.path.join(data_dir, "{}.h5".format(aq_name)), "w")
            if train:
                print("Writing {} data, valid count {}".format(aq_name, valid))
            else:
                if last_valid_dt is None:
                    print("{}\tno valid data.".format(aq_name))
                    continue
                history_aq = [history_aq[-1]]
                history_grid = [history_grid[-1]]
                forecast = [forecast[-1]]
                timestamps = [timestamps[-1]]
                print("{}\tlast valid data\t{}".format(aq_name, last_valid_dt))
            h5_file.create_dataset("grid", data=np.moveaxis(np.array(history_grid), 3, 1))
            h5_file.create_dataset("history", data=np.array(history_aq))
            h5_file.create_dataset("predict", data=np.array(predict_aq))
            h5_file.create_dataset("timestep", data=np.array(timestamps))
            h5_file.create_dataset("weather_forecast", data=np.array(forecast))
            h5_file.flush()
            h5_file.close()


if __name__ == "__main__":
    export_data(city="bj",
                start=["2017-01-01-00"],
                end=["2018-05-01-00"],
                train=True, fill=False,
                history_length=48, predict_length=50, grid_length=7, time_delta=1)
