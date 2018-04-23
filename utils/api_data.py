import requests
from datetime import datetime
import sys
import os
import csv


def datetime_string_convert(source):
    return (datetime.strptime(source, format_string[0])).strftime(format_string[1])


format_string = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d-%H"]
start, end = "2018-04-01 00:00:00", "2018-04-22 00:00:00"
[start_string, end_string] = list(map(datetime_string_convert, [start, end]))


def fetch_data(url, file_header):
    responses = requests.get(url)
    rows = responses.text.split('\n')
    aggregate = len(rows)
    data_dir = "../data/api"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    with open("".join([data_dir, "/" + file_header + "_", start_string,
                       "_", end_string, ".csv"]), "w", newline='') as file:
        writer = csv.writer(file, delimiter=',')
        for i in range(0, aggregate - 1):
            row = rows[i].rstrip().split(",")[1:]
            writer.writerow(row)
        print("Finished fetching", file_header, "data.")


if __name__ == '__main__':
    url = "https://biendata.com/competition/meteorology/bj_grid/" + \
          start_string + "/" + end_string + "/2k0d1d8"
    fetch_data(url, "meo")
    # url = "https://biendata.com/competition/airquality/bj/" + \
    #       start_string + "/" + end_string + "/2k0d1d8"
    # fetch_data(url, "aq")
