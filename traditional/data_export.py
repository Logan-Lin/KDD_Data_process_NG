import copy
import csv
import math
from datetime import datetime, timedelta

from deepkdd import raw_fetch
from deepkdd.tools import per_delta


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
    try:
        meo_row = grid_dicts[get_nearest(main_id)][dt_string]
        wind_direction = float(meo_row[3]) / 360 * 2 * math.pi
        wind_speed = float(meo_row[4])
        angle = abs(cal_angle(main_coordinate, verse_coordinate) - wind_direction)
        return [math.cos(angle * wind_speed) / distance] + meo_row
    except KeyError:
        return [None] * 6


aq_location, grid_location, aq_dicts, grid_dicts = raw_fetch.load_all()

# Load holiday date list
format_string = "%Y-%m-%d %H:%M:%S"
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
head_row = ["aq_station", "time", "weekday", "workday", "holiday"] + aq_row + \
           ["temperature", "pressure", "humidity", "wind_direction", "wind_speed"]
for i in range(34):
    head_row.append("near_aq_factor_" + str(i))
    head_row = head_row + aq_row

# Export data
for aq_name in aq_location.keys():
    export_file = open("../data/export/" + aq_name + ".csv", "w", newline='')
    writer = csv.writer(export_file, delimiter=',')
    writer.writerow(head_row)
    start_datetime, end_datetime = datetime.strptime("2017-01-01 14:00:00", format_string), \
                                   datetime.strptime("2018-03-27 05:00:00", format_string)
    for dt_object in per_delta(start_datetime, end_datetime, timedelta(hours=1)):
        try:
            row = list()
            dt_string = dt_object.strftime(format_string)
            row += [aq_name] + [dt_string] + [dt_object.weekday()] + \
                   [[1, 0][dt_object.weekday() in range(5)]] + \
                   [[0, 1][dt_object.date in holiday_array]]
            row += (aq_dicts[aq_name][dt_string])
            nearest_grid = get_nearest(aq_name)
            row += (grid_dicts[nearest_grid][dt_string])

            other_aq = copy.copy(aq_location)
            del other_aq[aq_name]

            for other_aq_id in other_aq.keys():
                row += cal_affect_factor(other_aq_id, aq_name, dt_string)
            writer.writerow(row)
            export_file.flush()
            print("Exporting", aq_name, dt_string, "data")
        except KeyError as e:
            print("Data Loss", dt_object.strftime(format_string), aq_name, e, sep=' | ')
    export_file.close()
