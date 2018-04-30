import csv
import os
import shutil
from copy import copy
from datetime import datetime, timedelta
from time import sleep, time
from utils import bj_raw_fetch, ld_raw_fetch, tools
import operator

import requests

bj_aq_location = bj_raw_fetch.aq_location
bj_grid_location = bj_raw_fetch.grid_location
ld_aq_location = ld_raw_fetch.aq_location
ld_grid_location = ld_raw_fetch.grid_location
location_dict = {"ld": {"aq": ld_aq_location, "meo": ld_grid_location},
                 "bj": {"aq": bj_aq_location, "meo": bj_grid_location}}
data_column_scope = {"ld": 3, "bj": 6}
grid_column_scope = 5


def datetime_string_convert(source):
    return (datetime.strptime(source, format_string[0]) - timedelta(hours=9)).strftime(format_string[1])


def get_start_end(end_object):
    return (end_object - timedelta(hours=48)).strftime(format_string[1]), end_object.strftime(format_string[1])


format_string = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d-%H"]


def fetch_data(file_header, city, start_str, end_str, stream=False, timeout=30):
    if file_header not in ["meo", "aq"]:
        print("File header must be 'meo' or 'aq'!")
        exit(1)

    real_city = copy(city)
    type_str = {"aq": "airquality", "meo": "meteorology"}[file_header]
    if file_header == 'meo':
        city += "_grid"
    url = "https://biendata.com/competition/{}/{}/{}/{}/2k0d1d8".format(type_str, city, start_str, end_str)

    valid = False
    while not valid:
        try:
            if stream:
                start = time()
                r = requests.get(url=url, timeout=timeout, stream=True)
                r.raise_for_status()
                content = bytes()
                content_gen = r.iter_content(1024)
                while True:
                    if time() - start > timeout:
                        raise TimeoutError('Time out! ({} seconds)'.format(timeout))
                    try:
                        content += next(content_gen)
                    except StopIteration:
                        break
                data = content.decode()
                if len(data) in [0, 1]:
                    raise ValueError('Bad requests data')
                r.close()
                valid = True
            else:
                data = requests.get(url=url, timeout=timeout, stream=False)
        except:
            valid = False
            print("Fetching failed... Trying again")
            sleep(1)
    print("Finished fetching", real_city, file_header, start_str, end_string, "data")
    rows = data.split('\n')
    aggregate = len(rows)
    data_dir = "data_{}_api/{}/{}_{}".format(real_city, file_header, start_str, end_str)
    if aggregate > 1:
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        else:
            shutil.rmtree(data_dir)
            os.makedirs(data_dir)
    for i in range(1, aggregate - 1):
        row = rows[i].rstrip().split(",")[1:]
        with open(data_dir + "/" + row[0] + ".csv", "a", newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            writer.writerow(row[1:])
            csv_file.flush()
    print("Finished writing", real_city, file_header, start_str, end_string, "data")


def float_m(value):
    if value is None or len(value) == 0:
        return None
    else:
        number = float(value)
        if number < 0:
            return None
        else:
            return number


def float_zero(value):
    if value is None or len(value) == 0:
        return None
    else:
        return float(value)


def fill_api_data(city, data_type, start_str, end_str):
    directory = "data_{}_api/{}/{}_{}".format(city, data_type, start_str, end_str)
    location = location_dict[city][data_type]
    data_dicts = dict()
    errors = []

    start_obj, end_obj = datetime.strptime(start_str, format_string[1]), \
                         datetime.strptime(end_str, format_string[1])

    modified_directory = "data_{}_api_m/{}/{}_{}".format(city, data_type, start_str, end_str)
    if not os.path.exists(modified_directory):
        os.makedirs(modified_directory)
    else:
        shutil.rmtree(modified_directory)
        os.makedirs(modified_directory)

    for location_name in location.keys():
        filled_count = 0

        data_dict = dict()
        with open("{}/{}.csv".format(directory, location_name), "r") as csv_file:
            reader = csv.reader(csv_file, delimiter=',')
            for row in reader:
                data_dict[row[0]] = list(map(float_m, row[1:data_column_scope[city] + 1]))

            # Fill timestamp loss with None
            for dt_obj in tools.per_delta(start_obj, end_obj, timedelta(hours=1)):
                dt_str = dt_obj.strftime(format_string[0])
                try:
                    data_dict[dt_str]
                except KeyError:
                    data_dict[dt_str] = [None] * data_column_scope[city]

            # Fill data if possible
            dt_obj = start_obj
            while dt_obj < end_obj:
                dt_obj += timedelta(hours=1)
                dt_str = dt_obj.strftime(format_string[0])
                current_data = data_dict[dt_str]
                for column in range(data_column_scope[city]):
                    try:
                        if current_data[column] is None:
                            # Found None value, begin counting the length of empty data
                            count = 1
                            while True:
                                if data_dict[(dt_obj + timedelta(hours=count)).
                                        strftime(format_string[0])][column] is None:
                                    count += 1
                                else:
                                    break
                            if count > 3:
                                raise KeyError("Too much data is lost.")
                            start_value = data_dict[(dt_obj - timedelta(hours=1)).
                                strftime(format_string[0])][column]
                            if start_value is None:
                                raise KeyError("Data is empty in the first row.")
                            end_value = data_dict[(dt_obj + timedelta(hours=count)).
                                strftime(format_string[0])][column]
                            gradient = (end_value - start_value) / (count + 1)
                            for i in range(count):
                                data_dict[(dt_obj + timedelta(hours=i)).
                                    strftime(format_string[0])][column] = start_value + (i + 1) * gradient
                                filled_count += 1
                    except KeyError as e:
                        errors.append(e)
                        continue
        data_dicts[location_name] = data_dict

        sorted_data_matrix = sorted(data_dict.items(), key=operator.itemgetter(0))
        with open("{}/{}.csv".format(modified_directory, location_name), "w", newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            for dt_str, data in sorted_data_matrix:
                writer.writerow([dt_str] + data)
            csv_file.flush()
        print("Finished processing {} {} {} csv, filled {}".format(
            location_name, start_string, end_string, filled_count))


def fill_api_data_all(city, data_type, start_str, end_str):
    directory = "data_{}_api/{}/{}_{}".format(city, data_type, start_str, end_str)
    location = location_dict[city][data_type]
    data_dicts = dict()
    errors = []

    start_obj, end_obj = datetime.strptime(start_str, format_string[1]), \
                         datetime.strptime(end_str, format_string[1])

    modified_directory = "data_{}_api_m/{}/{}_{}".format(city, data_type, start_str, end_str)
    if not os.path.exists(modified_directory):
        os.makedirs(modified_directory)
    else:
        shutil.rmtree(modified_directory)
        os.makedirs(modified_directory)

    for location_name in location.keys():

        filled_count = 0
        empty_count = 0

        data_dict = dict()
        with open("{}/{}.csv".format(directory, location_name), "r") as csv_file:
            reader = csv.reader(csv_file, delimiter=',')
            for row in reader:
                data_dict[row[0]] = list(map(float_zero, row[2:grid_column_scope + 2]))

            # Fill timestamp loss with None
            for dt_obj in tools.per_delta(start_obj, end_obj, timedelta(hours=1)):
                dt_str = dt_obj.strftime(format_string[0])
                try:
                    data_dict[dt_str]
                except KeyError:
                    data_dict[dt_str] = [None] * grid_column_scope
                    empty_count += 1

            # Fill data if possible
            dt_obj = start_obj
            while dt_obj < end_obj:
                dt_obj += timedelta(hours=1)
                dt_str = dt_obj.strftime(format_string[0])
                current_data = data_dict[dt_str]
                for column in range(grid_column_scope):
                    try:
                        if current_data[column] is None:
                            # Found None value, begin counting the length of empty data
                            count = 1
                            while True:
                                if data_dict[(dt_obj + timedelta(hours=count)).
                                        strftime(format_string[0])][column] is None:
                                    count += 1
                                else:
                                    break
                            start_value = data_dict[(dt_obj - timedelta(hours=1)).
                                strftime(format_string[0])][column]
                            if start_value is None:
                                raise KeyError("Data is empty in the first row.")
                            end_value = data_dict[(dt_obj + timedelta(hours=count)).
                                strftime(format_string[0])][column]
                            gradient = (end_value - start_value) / (count + 1)
                            for i in range(count):
                                data_dict[(dt_obj + timedelta(hours=i)).
                                    strftime(format_string[0])][column] = start_value + (i + 1) * gradient
                                filled_count += 1
                    except KeyError as e:
                        errors.append(e)
                        continue
        data_dicts[location_name] = data_dict

        sorted_data_matrix = sorted(data_dict.items(), key=operator.itemgetter(0))
        with open("{}/{}.csv".format(modified_directory, location_name), "w", newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            for dt_str, data in sorted_data_matrix:
                writer.writerow([dt_str] + data)
            csv_file.flush()
        print("Finished processing {} {} {} csv, filled {}, empty row {}".format(
            location_name, start_string, end_string, filled_count / grid_column_scope, empty_count))


def is_invalid(data):
    if data is None or len(data) == 0 or data < 0:
        return True
    else:
        return False


if __name__ == '__main__':
    start_string, end_string = "2018-04-01-00", "2018-04-29-22"

    # fetch_data("aq", "bj", start_string, end_string, True)
    # fetch_data("meo", "bj", start_string, end_string, True, 600)
    # fetch_data("aq", "ld", start_string, end_string, True)
    # fetch_data("meo", "ld", start_string, end_string, True, 1200)

    fill_api_data("bj", "aq", start_string, end_string)
    fill_api_data("ld", "aq", start_string, end_string)
    fill_api_data_all("bj", "meo", start_string, end_string)
    fill_api_data_all("ld", "meo", start_string, end_string)
