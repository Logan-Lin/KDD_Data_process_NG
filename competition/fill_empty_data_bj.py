import csv
import operator
from os import listdir, makedirs
from os.path import isfile, join, splitext, exists
from utils import bj_raw_fetch, ld_raw_fetch
from copy import copy


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

    for aq_name in predict_dicts.keys():
        filled_dict = filled_dicts[aq_name]
        predict_dict = predict_dicts[aq_name]
        for dt_string in filled_dict.keys():
            if dt_string in predict_dict:
                for column in range(column_span[city]):
                    if filled_dict[dt_string][column] is None:
                        filled_dict[dt_string][column] = predict_dict[dt_string][column]
    return filled_dicts


def write_filled_dicts(filled_dicts, start_string, end_string, city):
    directory = "{}/{}_{}".format(filled_directory_dict[city], start_string, end_string)
    if not exists(directory):
        makedirs(directory)
    for aq_name in filled_dicts.keys():
        sorted_filled_matrix = sorted(filled_dicts[aq_name].items(), key=operator.itemgetter(0))
        with open("{}/{}.csv".format(directory, aq_name), "w", newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            for dt_string, data in sorted_filled_matrix:
                writer.writerow([dt_string] + data)
            csv_file.flush()
        print("Filled {}".format(aq_name))


if __name__ == '__main__':
    start_string, end_string = "2018-04-29-20", "2018-04-30-22"
    # Proceed Beijing data
    bj_original_aq_dicts = bj_raw_fetch.load_aq_dicts_none(start_string, end_string)
    bj_aq_dicts = get_aq_dicts(predict_directory_dict["bj"], "bj")
    bj_filled_dicts = get_filled_data(bj_original_aq_dicts, bj_aq_dicts, "bj")
    write_filled_dicts(bj_filled_dicts, start_string, end_string, "bj")
