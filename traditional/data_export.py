import sys
sys.path.append("../")

import argparse
import math
import os
from datetime import datetime, timedelta
from time import sleep
from competition import load_data

import h5py
import numpy as np

from forecast import parse
from utils import ld_raw_fetch, export_statistic, bj_raw_fetch
from utils.tools import per_delta, get_one_hot, angle_to_int, str2bool

history_span = 24
predict_span = 50


def get_time_string(start_time_s, end_time_s, time_delta=timedelta(hours=1)):
    time_string_array = []
    start_time = datetime.strptime(start_time_s, format_string)
    end_time = datetime.strptime(end_time_s, format_string)
    for time in per_delta(start_time, end_time, time_delta):
        time_string_array.append(time.strftime(format_string))
    return time_string_array


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


def cal_distance(coordinate1, coordinate2):
    return math.sqrt(math.pow((float(coordinate1[0]) - float(coordinate2[0])), 2) +
                     math.pow((float(coordinate1[1]) - float(coordinate2[1])), 2))


def cal_angle(main, verse):
    temp_angle = math.acos((float(verse[1]) - float(main[1])) / cal_distance(main, verse))
    if float(main[0]) - float(verse[0]) < 0:
        temp_angle = 2 * math.pi - temp_angle
    return temp_angle


def cal_affect_factor(main_id, verse_id, dt_string):
    main_coordinate, verse_coordinate = aq_location[main_id], aq_location[verse_id]
    distance = cal_distance(main_coordinate, verse_coordinate)
    meo_row = grid_dicts[get_nearest(main_id)][dt_string]
    wind_direction = float(meo_row[3]) / 360 * 2 * math.pi
    wind_speed = float(meo_row[4])
    angle = abs(cal_angle(main_coordinate, verse_coordinate) - wind_direction)
    return math.cos(angle * wind_speed) / distance


def get_fake_forecast_data(aq_name, dt_string):
    grid_id, grid_coor = get_nearest(aq_location[aq_name])
    data_row = grid_dicts[grid_id][dt_string]
    # Temperature, humidity, wind direction, wind speed
    return [data_row[0], data_row[2], data_row[4]] + get_one_hot(angle_to_int(data_row[3]), 16)


def get_forecast_data(dt_object):
    file_dir = {"bj": "../forecast_data/ld_{}.txt", "ld": "../forecast_data/bj_{}.txt"}[ci]
    file_directory = file_dir.format((dt_object + timedelta(hours=8)).strftime("%m_%d_%H"))
    return parse.get_data(file_directory)


def check_valid(aq_name, start_object):
    try:
        aq_dict = aq_dicts[aq_name]
        history_aq_matrix, history_meo_matrix, forecast_matrix, predict_aq_matrix, statistic_matrix = [], [], [], [], []
        need_fake = False
        try:
            forecast_matrix = get_forecast_data(start_object)
        except FileNotFoundError:
            need_fake = True
        for i in range(history_span - 1, -1, -1):
            valid_dt_string = (start_object - timedelta(hours=i)).strftime(format_string)
            history_aq_matrix.append(aq_dict[valid_dt_string])
            history_meo_matrix.append(grid_dicts[(get_nearest(aq_location[aq_name]))[0]][valid_dt_string])
        for i in range(1, predict_span + 1):
            predict_dt_string = (start_object + timedelta(hours=i)).strftime(format_string)
            column = None
            if ci == "bj":
                column="o3"
            statistic_matrix.append(export_statistic.get_month_data(aq_name, predict_dt_string, df, column=column))
            if export_predict:
                if ci == "bj":
                    predict_aq_matrix.append([aq_dict[predict_dt_string][4]])
                else:
                    predict_aq_matrix.append(aq_dict[predict_dt_string])
            if need_fake:
                forecast_matrix.append(get_fake_forecast_data(aq_name, predict_dt_string))
    except KeyError:
        return [None] * 8
    return history_aq_matrix, history_meo_matrix, forecast_matrix, predict_aq_matrix, \
           start_object.weekday(), [1, 0][start_object.weekday() in range(5)], start_object.timestamp(), \
           statistic_matrix


# Load holiday date list
format_string = "%Y-%m-%d %H:%M:%S"
format_string_2 = "%Y-%m-%d-%H"
holiday_start_ends = [["2017-01-01", "2017-01-02"],
                      ["2017-01-27", "2017-02-02"],
                      ["2017-04-02", "2017-04-04"],
                      ["2017-04-29", "2017-05-01"],
                      ["2017-05-28", "2017-05-30"],
                      ["2017-10-01", "2017-10-08"],
                      ["2018-01-01", "2018-01-01"],
                      ["2018-02-15", "2018-02-21"],
                      ["2018-04-05", "2018-04-07"],
                      ["2018-04-29", "2018-05-01"]]
holiday_array = []
for i in range(len(holiday_start_ends)):
    for j in range(2):
        holiday_start_ends[i][j] += " 00:00:00"
    holiday_array += get_time_string(holiday_start_ends[i][0], holiday_start_ends[i][1], timedelta(days=1))
for i in range(len(holiday_array)):
    holiday_array[i] = datetime.strptime(holiday_array[i], format_string).date()


# Export data
def export_data(city, read_start_string, read_end_string, export_start_string, export_end_string,
                use_fill, use_history, export_train):
    start_string, end_string = read_start_string, read_end_string
    global ci
    ci = city
    global aq_location, grid_location, aq_dicts, grid_dicts, df
    if use_history:
        if city == "ld":
            aq_location, grid_location, aq_dicts, grid_dicts = ld_raw_fetch.load_all_history()
            df = load_data.load_directory_data(load_data.history_data_directory[city]["aq"],
                                               load_data.data_header_dict[city]["aq"], drop=["no2"])
        elif city == "bj":
            aq_location, grid_location, aq_dicts, grid_dicts = bj_raw_fetch.load_all_history()
            df = load_data.load_directory_data(load_data.history_data_directory[city]["aq"],
                                               load_data.data_header_dict[city]["aq"])

    else:
        if city == "ld":
            aq_location, grid_location, aq_dicts, grid_dicts = ld_raw_fetch.load_all(start_string, end_string)
            df = load_data.load_directory_data([load_data.filled_data_directory[city],
                                                load_data.history_data_directory[city]["aq"]],
                                               load_data.data_header_dict[city]["aq"], drop=["no2"])
        elif city == "bj":
            aq_location, grid_location, aq_dicts, grid_dicts = bj_raw_fetch.load_all(start_string, end_string)
            df = load_data.load_directory_data([load_data.filled_data_directory[city],
                                                load_data.history_data_directory[city]["aq"]],
                                               load_data.data_header_dict[city]["aq"])

        if use_fill:
            if city == "ld":
                aq_dicts = ld_raw_fetch.load_filled_dicts(start_string, end_string)
            elif city == "bj":
                aq_dicts = bj_raw_fetch.load_filled_dicts(start_string, end_string)
    global export_predict
    export_predict = export_train

    if export_start_string is not None:
        start_string, end_string = export_start_string, export_end_string
    start_datetime, end_datetime = datetime.strptime(start_string, format_string_2), \
                                   datetime.strptime(end_string, format_string_2)
    diff = end_datetime - start_datetime
    days, seconds = diff.days, diff.seconds
    delta_time = int(days * 24 + seconds // 3600)
    if export_train:
        delta_time = int(delta_time / 24)

    directory = ""
    if export_train:
        if city == "ld":
            directory = "../data_ld/tradition_train/{}_{}".format(start_string, end_string)
        elif city == "bj":
            directory = "../data/tradition_train/{}_{}".format(start_string, end_string)
    else:
        if city == "ld":
            directory = "../data_ld/tradition_predict/{}_{}".format(start_string, end_string)
        elif city == "bj":
            directory = "../data/tradition_predict/{}_{}".format(start_string, end_string)
    if not os.path.exists(directory):
        os.makedirs(directory)

    print("\nExporting to {}".format(directory))
    # out_file = open("out{}_{}.txt".format(start_string, end_string), "w")
    for aq_name in aq_location.keys():
        # if aq_name not in ["KF1"]:
        #     continue
        timestamp_matrix, history_aq, history_meo, forecast, predict_aq, statistic = [], [], [], [], [], []
        if export_train:
            aggregate = 0
            valid = 0

            for dt_object in per_delta(start_datetime, end_datetime, timedelta(hours=24)):
                aggregate += 1
                if aggregate % 10 == 0:
                    print("\t{} exported %3.2f%%".format(aq_name) % (100 * aggregate / delta_time))
                #     out_file.write("{} exported %3.2f%%\n".format(aq_name) % (100 * aggregate / delta_time))
                #     out_file.flush()

                history_aq_matrix, history_meo_matrix, forecast_matrix, predict_matrix, \
                weekday, weekend, timestamp, statistic_matrix = check_valid(aq_name, dt_object)
                if history_aq_matrix is None:
                    continue

                timestamp_matrix.append([timestamp, weekday, weekend])
                history_aq.append(history_aq_matrix)
                history_meo.append(history_meo_matrix)
                forecast.append(forecast_matrix)
                predict_aq.append(predict_matrix)
                statistic.append(statistic_matrix)
                valid += 1

            h5_file = h5py.File("{}/{}.h5".format(directory, aq_name), "w")
            h5_file.create_dataset("timestamp", data=np.array(timestamp_matrix))
            h5_file.create_dataset("history_aq", data=np.array(history_aq))
            h5_file.create_dataset("history_meo", data=np.array(history_meo))
            h5_file.create_dataset("forecast", data=np.array(forecast))
            h5_file.create_dataset("predict_aq", data=np.array(predict_aq))
            h5_file.create_dataset("statistic", data=np.array(statistic))
            h5_file.flush()
            h5_file.close()
            print("{} finished, valid {}".format(aq_name, valid))
            sleep(0.1)
        else:
            last_valid_dt = None
            for dt_object in per_delta(start_datetime, end_datetime, timedelta(hours=24)):
                history_aq_matrix, history_meo_matrix, forecast_matrix, predict_matrix, \
                weekday, weekend, timestamp, statistic_matrix = check_valid(aq_name, dt_object)
                if history_aq_matrix is None:
                    continue

                timestamp_matrix = [[timestamp, weekday, weekend]]
                history_aq = [history_aq_matrix]
                history_meo = [history_meo_matrix]
                forecast = [forecast_matrix]
                predict_aq = [predict_matrix]
                statistic = [statistic_matrix]
                last_valid_dt = dt_object
            if last_valid_dt is not None:
                h5_file = h5py.File("{}/{}.h5".format(directory, aq_name), "w")
                h5_file.create_dataset("timestamp", data=np.array(timestamp_matrix))
                h5_file.create_dataset("history_aq", data=np.array(history_aq))
                h5_file.create_dataset("history_meo", data=np.array(history_meo))
                h5_file.create_dataset("forecast", data=np.array(forecast))
                h5_file.create_dataset("predict_aq", data=np.array(predict_aq))
                h5_file.create_dataset("statistic", data=np.array(statistic))
                h5_file.flush()
                h5_file.close()
                print("{} last valid {}".format(aq_name, last_valid_dt.strftime(format_string_2)))
            else:
                print("{} no valid data".format(aq_name))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--city", type=str,
                        help="City, input 'bj' or 'ld' for Beijing and London", default="bj")
    parser.add_argument("-s", "--start", type=str,
                        help="Start datetime string, in YYYY-MM-DD-hh format", default="2017-01-01-22")
    parser.add_argument("-e", "--end", type=str,
                        help="End datetime string, in YYYY-MM-DD-hh format", default="2018-04-21-22")
    parser.add_argument("-es", "--exportstart", type=str,
                        help="Start datetime to export, in YYYY-MM-DD-hh format", default=None)
    parser.add_argument("-ee", "--exportend", type=str,
                        help="End datetime to export, in YYYY-MM-DD-hh format", default=None)
    parser.add_argument("-f", "--fill", type=str2bool,
                        help="Use filled data or not, input true/false", default=False)
    parser.add_argument("-his", "--history", type=str2bool,
                        help="Use history data", default=True)
    parser.add_argument("-t", "--train", type=str2bool,
                        help="Export train data or predict data", default=True)
    argv = parser.parse_args()

    export_data(argv.city, argv.start, argv.end, argv.exportstart, argv.exportend, argv.fill, argv.history, argv.train)