import copy
import math
import time as ti
from datetime import datetime, timedelta
from progressbar import ProgressBar as PB, Bar, Percentage
import operator

import h5py
import numpy as np

from utils import bj_raw_fetch
from utils.tools import per_delta


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


start_string, end_string = "2018-04-26-22", "2018-04-27-22"
aq_location, grid_location, aq_dicts_, grid_dicts = bj_raw_fetch.load_all(start_string, end_string)
aq_dicts = bj_raw_fetch.load_filled_dicts(start_string, end_string)

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
# for i in range(34):
#     head_row.append("near_aq_factor_" + str(i))
#     head_row = head_row + aq_row

# Export data
h5_file = h5py.File("../data/tradition_export/traditional.h5", "w")
print("\nFetching data to export...")
for aq_name in aq_location.keys():
    # export_file = open("../data/tradition_export/" + aq_name + ".csv", "w", newline='')
    # writer = csv.writer(export_file, delimiter=',')
    # writer.writerow(head_row)
    valid_count = 0
    aggregate = 0

    start_datetime, end_datetime = datetime.strptime(start_string, format_string_2), \
                                   datetime.strptime(end_string, format_string_2)
    diff = end_datetime - start_datetime
    days, seconds = diff.days, diff.seconds
    delta_time = int(days * 24 + seconds // 3600)

    ti.sleep(0.1)
    bar = PB(initial_value=0, maxval=delta_time, widgets=[aq_name, ' ', Bar('=', '[', ']'), ' ', Percentage()])

    data_matrix = []
    for dt_object in per_delta(start_datetime, end_datetime, timedelta(hours=1)):
        bar.update(aggregate)
        aggregate += 1
        try:
            row = list()
            dt_string = dt_object.strftime(format_string)
            row += [int(dt_object.timestamp())] + [dt_object.weekday()] + \
                   [[1, 0][dt_object.weekday() in range(5)]] + \
                   [[0, 1][dt_object.date in holiday_array]]
            row += (aq_dicts[aq_name][dt_string])
            nearest_grid = get_nearest(aq_name)
            row += (grid_dicts[nearest_grid][dt_string])

            other_aq = copy.copy(aq_location)
            del other_aq[aq_name]

            factor_dict = dict()
            for other_aq_id in other_aq.keys():
                factor = cal_affect_factor(other_aq_id, aq_name, dt_string)
                factor_dict[other_aq_id] = factor
            sorted_factor_dict = sorted(factor_dict.items(), key=operator.itemgetter(1), reverse=True)
            valid = False
            other_aq_row = [None] * 6
            for other_aq_id, factor in sorted_factor_dict:
                if factor < 0:
                    valid = False
                    break
                try:
                    other_aq_row = aq_dicts[other_aq_id][dt_string]
                    valid = True
                except KeyError:
                    valid = False
                if valid:
                    row += [factor] + other_aq_row
                    break
            if not valid:
                raise KeyError("Data loss here")

            data_matrix.append(row)
            valid_count += 1
        except KeyError as e:
            pass
    print(" - valid%6.2f%%" % (100 * valid_count / aggregate), sep='')
    h5_file.create_dataset(aq_name, data=np.asarray(data_matrix))
    ti.sleep(0.1)
h5_file.flush()
h5_file.close()
# export_file.close()
