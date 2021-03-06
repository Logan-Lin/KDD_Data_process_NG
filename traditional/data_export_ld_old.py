import sys
sys.path.append("../")

import argparse
import math
from datetime import datetime, timedelta

import h5py
import numpy as np

from utils import ld_raw_fetch
from utils.tools import per_delta, str2bool

aq_location, grid_location, aq_dicts, grid_dicts = dict(), dict(), dict(), dict()


def get_time_string(start_time_s, end_time_s, time_delta=timedelta(hours=1)):
    time_string_array = []
    start_time = datetime.strptime(start_time_s, format_string)
    end_time = datetime.strptime(end_time_s, format_string)
    for time in per_delta(start_time, end_time, time_delta):
        time_string_array.append(time.strftime(format_string))
    return time_string_array


def get_nearest(aq_name):
    coor = aq_location[aq_name]
    min_distance = 999999
    min_grid = ""
    for key in grid_location.keys():
        grid_coor = grid_location[key]
        dist = math.sqrt(math.pow(float(grid_coor[0]) - float(coor[0]), 2) +
                         (math.pow(float(grid_coor[1]) - float(coor[1]), 2)))
        if min_distance > dist:
            min_distance = dist
            min_grid = key
    return min_grid


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

# Load csv header row list
aq_row = ["PM2.5", "PM10", "NO2", "CO", "O3", "SO2"]
head_row = ["time", "weekday", "workday", "holiday"] + aq_row + \
           ["temperature", "pressure", "humidity", "wind_direction", "wind_speed"] + \
           ["near_aq_factor"] + aq_row


def export_data(read_start_string, read_end_string, export_start_string=None,
                export_end_string=None, use_fill=True):
    start_string, end_string = read_start_string, read_end_string
    global aq_location, grid_location, aq_dicts, grid_dicts
    aq_location, grid_location, aq_dicts, grid_dicts = ld_raw_fetch.load_all(start_string, end_string)
    if use_fill:
        aq_dicts = ld_raw_fetch.load_filled_dicts(start_string, end_string)

    if export_start_string is not None:
        start_string, end_string = export_start_string, export_end_string
    h5_file = h5py.File("../data_ld/tradition_export/traditional_ld_{}_{}.h5".format(start_string, end_string), "w")
    print("\nFetching data to export...")
    for aq_name in aq_location.keys():
        start_datetime, end_datetime = datetime.strptime(start_string, format_string_2), \
                                       datetime.strptime(end_string, format_string_2)

        last_valid_dt_object = None
        data_to_write = []
        for dt_object_day in per_delta(start_datetime, end_datetime, timedelta(days=1)):
            have_valid = False
            data_matrix = []

            for dt_object in per_delta(dt_object_day - timedelta(hours=23), dt_object_day, timedelta(hours=1)):
                try:
                    row = list()
                    dt_string = dt_object.strftime(format_string)

                    row += [dt_object.timestamp()] +\
                    [dt_object.weekday()] + \
                    [[1, 0][dt_object.weekday() in range(5)]]
                    # + \
                    # [[0, 1][dt_object.date in holiday_array]]
                    row += (aq_dicts[aq_name][dt_string])
                    nearest_grid = get_nearest(aq_name)
                    row += (grid_dicts[nearest_grid][dt_string])

                    # other_aq = copy.copy(aq_location)
                    # del other_aq[aq_name]
                    #
                    # factor_dict = dict()
                    # for other_aq_id in other_aq.keys():
                    #     factor = cal_affect_factor(other_aq_id, aq_name, dt_string)
                    #     factor_dict[other_aq_id] = factor
                    # sorted_factor_dict = sorted(factor_dict.items(), key=operator.itemgetter(1), reverse=True)
                    # valid = False
                    # other_aq_row = [None] * 2
                    # for other_aq_id, factor in sorted_factor_dict:
                    #     if factor < 0:
                    #         valid = False
                    #         break
                    #     try:
                    #         other_aq_row = aq_dicts[other_aq_id][dt_string]
                    #         valid = True
                    #     except KeyError:
                    #         valid = False
                    #     if valid:
                    #         row += [factor] + other_aq_row
                    #         break
                    # if not valid:
                    #     raise KeyError("Data loss here")

                    data_matrix.append(row)
                    have_valid = True

                except KeyError as e:
                    have_valid = False
                    break
            if have_valid:
                last_valid_dt_object = dt_object_day
                data_to_write = data_matrix
        if last_valid_dt_object is not None:
            print("{} last valid data - {}".format(aq_name, last_valid_dt_object.strftime(format_string_2)))
            h5_file.create_dataset(aq_name, data=np.asarray(data_to_write))
        else:
            print("{} has no valid data".format(aq_name))
    h5_file.flush()
    h5_file.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start", type=str,
                        help="Start datetime string, in YYYY-MM-DD-hh format", default="2018-04-30-22")
    parser.add_argument("-e", "--end", type=str,
                        help="End datetime string, in YYYY-MM-DD-hh format", default="2018-05-01-22")
    parser.add_argument("-es", "--exportstart", type=str,
                        help="Start datetime to export, in YYYY-MM-DD-hh format", default=None)
    parser.add_argument("-ee", "--exportend", type=str,
                        help="End datetime to export, in YYYY-MM-DD-hh format", default=None)
    parser.add_argument("-f", "--fill", type=str2bool,
                        help="Use filled data or not, input true/false", default=True)
    argv = parser.parse_args()

    export_data(argv.start, argv.end, argv.exportstart, argv.exportend, argv.fill)
