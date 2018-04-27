import csv
import os
import shutil
from copy import copy
from datetime import datetime, timedelta
from time import sleep, time

import requests


def datetime_string_convert(source):
    return (datetime.strptime(source, format_string[0]) - timedelta(hours=9)).strftime(format_string[1])


def get_start_end(end_object):
    return (end_object - timedelta(hours=25)).strftime(format_string[1]), end_object.strftime(format_string[1])


format_string = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d-%H"]
# start, end = "2018-04-24 06:00:00", "2018-04-25 07:00:00"
# [start_string, end_string] = list(map(datetime_string_convert, [start, end]))


def fetch_data(file_header, city, start_str, end_str):
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
            start = time()
            timeout = 30
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
            data = content.decode().split('\n')
            if len(data) in [0, 1]:
                raise ValueError('Bad requests data')
            print("Finished fetching", real_city, file_header, start_str, end_string, "data")
            r.close()
            valid = True
        except (TimeoutError, ValueError, requests.exceptions.ConnectTimeout):
            valid = False
            print("Fetching failed... Trying again")
            sleep(1)
    rows = data
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


if __name__ == '__main__':
    while True:
        # now_dt_object = datetime.strptime("2018-04-25-07", format_string[1]) - timedelta(hours=9)
        now_dt_object = datetime.now() - timedelta(hours=9)
        if now_dt_object.hour is 22:
            print("Begin fetching", now_dt_object.strftime(format_string[1]), "data")
            start_string, end_string = get_start_end(now_dt_object)
            fetch_data("aq", "bj", start_string, end_string)
            fetch_data("meo", "bj", start_string, end_string)
            fetch_data("aq", "ld", start_string, end_string)
            fetch_data("meo", "ld", start_string, end_string)
            with open("out.txt", "a") as out_file:
                out_file.write(" ".join(["Finished", start_string, end_string, '\n']))
                out_file.flush()
            sleep(7200)
        else:
            with open("fail.txt", "a") as fail_file:
                fail_file.write((now_dt_object.strftime(format_string[0]) + " not valid\n"))
                fail_file.flush()
        sleep(600)
