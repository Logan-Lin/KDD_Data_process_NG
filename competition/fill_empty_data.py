import sys
sys.path.append("../")

import csv
import operator
from os import listdir, makedirs
from os.path import isfile, join, splitext, exists
from utils import bj_raw_fetch, ld_raw_fetch
from copy import copy
import argparse


column_span = {"bj": 6, "ld": 2}
predict_directory_dict = {"bj": "../data/prepare", "ld": "../data_ld/prepare"}
filled_directory_dict = {"bj": "../data_m/aq_filled", "ld": "../data_ld_m/aq_filled"}


# Extracts all aq data from files in given directory.
def get_aq_dicts(data_directory, city):
    # Get all csv files under given directory.
    all_files = [f for f in listdir(data_directory) if isfile(join(data_directory, f)) and splitext(f)[1] == '.csv']

    aq_dicts = dict()
    for file_name in all_files:
        aq_name = splitext(file_name)[0][9:]

        relative_file_directory = "{}/{}".format(data_directory, file_name)

        with open(relative_file_directory, "r") as csv_file:
            aq_dict = dict()
            reader = csv.reader(csv_file, delimiter=',')
            for row in reader:
                try:
                    aq_dict[row[0]] = list(map(float, row[1:column_span[city] + 1]))
                except ValueError as e:
                    print(e)
            aq_dicts[aq_name] = aq_dict
    return aq_dicts


def get_filled_data(original_dicts, predict_dicts, city):
    filled_dicts = copy(original_dicts)
    filled_counts = dict()

    for aq_name in predict_dicts.keys():
        filled_count = 0
        filled_dict = filled_dicts[aq_name]
        predict_dict = predict_dicts[aq_name]
        for dt_string in filled_dict.keys():
            if dt_string in predict_dict:
                for column in range(column_span[city]):
                    if filled_dict[dt_string][column] is None:
                        filled_dict[dt_string][column] = predict_dict[dt_string][column]
                        filled_count += 1
        filled_counts[aq_name] = filled_count
    return filled_dicts, filled_counts


def write_filled_dicts(filled_dicts, start_string, end_string, city, filled_counts):
    directory = "{}/{}_{}".format(filled_directory_dict[city], start_string, end_string)
    print("Filled data write to {}".format(directory))
    if not exists(directory):
        makedirs(directory)
    for aq_name in filled_dicts.keys():
        sorted_filled_matrix = sorted(filled_dicts[aq_name].items(), key=operator.itemgetter(0))
        with open("{}/{}.csv".format(directory, aq_name), "w", newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            for dt_string, data in sorted_filled_matrix:
                writer.writerow([dt_string] + data)
            csv_file.flush()
        print("Filled {} {} data".format(aq_name, filled_counts[aq_name]))


def fill_data(city, start_string, end_string):
    if city == "bj":
        bj_original_aq_dicts = bj_raw_fetch.load_aq_dicts_none(start_string, end_string)
        bj_aq_dicts = get_aq_dicts(predict_directory_dict["bj"], "bj")
        bj_filled_dicts, filled_counts = get_filled_data(bj_original_aq_dicts, bj_aq_dicts, "bj")
        write_filled_dicts(bj_filled_dicts, start_string, end_string, "bj", filled_counts)
    elif city == "ld":
        ld_original_aq_dicts = ld_raw_fetch.load_aq_dicts_none(start_string, end_string)
        ld_aq_dicts = get_aq_dicts(predict_directory_dict["ld"], "ld")
        ld_filled_dicts, filled_counts = get_filled_data(ld_original_aq_dicts, ld_aq_dicts, "ld")
        write_filled_dicts(ld_filled_dicts, start_string, end_string, "ld", filled_counts)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--city", type=str,
                        help="City, input 'bj' or 'ld' for Beijing and London", default="ld")
    parser.add_argument("-s", "--start", type=str,
                        help="Start datetime string, in YYYY-MM-DD-hh format", default="2018-04-30-22")
    parser.add_argument("-e", "--end", type=str,
                        help="End datetime string, in YYYY-MM-DD-hh format", default="2018-05-01-22")
    argv = parser.parse_args()

    fill_data(argv.city, argv.start, argv.end)
