import csv
from datetime import datetime, timedelta

from utils import bj_raw_fetch, tools, ld_raw_fetch


def float_m(value):
    if value is None or len(value) == 0:
        raise (ValueError("Data loss here"))
        # return None
    return float(value)


def csv_split(header):
    # open_file = "../data/Beijing_historical_meo_grid.csv"
    open_file = "../data/api/" + header + "_2018-04-01-00_2018-04-22-00.csv"
    with open(open_file, "r") as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        count = -1

        for row in reader:
            count += 1
            if count is 0:
                header_row = row
                continue
            with open("../data/" + header + "/" + row[0] + ".csv", "a", newline='') as file_for_writer:
                writer = csv.writer(file_for_writer, delimiter=',')
                writer.writerow([row[1]] + row[3:])
                file_for_writer.flush()
            if count % 10000 == 0:
                print("Processed", count, "row")


def ld_forecast_aq_split():
    open_file = "../data/London_historical_aqi_forecast_stations_20180331.csv"
    with open(open_file, "r") as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        count = -1

        for row in reader:
            count += 1
            if count is 0:
                header_row = row
                continue
            with open("../data_ld/aq/" + row[2] + ".csv", "a", newline='') as file_for_writer:
                writer = csv.writer(file_for_writer, delimiter=',')
                row_to_write = [row[1]] + row[3:]
                writer.writerow(row_to_write)
                file_for_writer.flush()
            if count % 10000 == 0:
                print("Processed", count, "row")


def ld_other_aq_split():
    open_file = "../data/London_historical_aqi_other_stations_20180331.csv"
    with open(open_file, "r") as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        count = -1

        for row in reader:
            count += 1
            if count is 0:
                header_row = row
                continue
            with open("../data_ld/aq/" + row[0] + ".csv", "a", newline='') as file_for_writer:
                writer = csv.writer(file_for_writer, delimiter=',')
                row_to_write = row[1:5]
                writer.writerow(row_to_write)
                file_for_writer.flush()
            if count % 10000 == 0:
                print("Processed", count, "row")


def split_bj_meo_api_data():
    open_file = "../data/api/meo_2018-04-01-00_2018-04-22-00.csv"
    with open(open_file, "r") as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        count = -1

        for row in reader:
            count += 1
            if count is 0:
                header_row = row
                continue
            with open("../data/meo/" + row[0] + ".csv", "a", newline='') as file_for_writer:
                writer = csv.writer(file_for_writer, delimiter=',')
                writer.writerow([row[1]] + row[3:])
                file_for_writer.flush()
            if count % 10000 == 0:
                print("Processed", count, "row")


def split_bj_aq_api_data():
    open_file = "../data/api/aq_2018-04-01-00_2018-04-22-00.csv"
    with open(open_file, "r") as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        count = -1

        for row in reader:
            count += 1
            if count is 0:
                header_row = row
                continue
            with open("../data/aq/" + row[0] + ".csv", "a", newline='') as file_for_writer:
                writer = csv.writer(file_for_writer, delimiter=',')
                writer.writerow(row[1:])
                file_for_writer.flush()
            if count % 1000 == 0:
                print("Processed", count, "row")


def split_ld_aq_api_data():
    open_file = "../data_ld/api/aq_2018-04-01-00_2018-04-22-00.csv"
    with open(open_file, "r") as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        count = -1

        for row in reader:
            count += 1
            if count is 0:
                header_row = row
                continue
            with open("../data_ld/aq/" + row[0] + ".csv", "a", newline='') as file_for_writer:
                writer = csv.writer(file_for_writer, delimiter=',')
                row_to_write = [format_ld_dt_string(datetime.strptime(row[1], format_string_m))] + row[2:5]
                writer.writerow(row_to_write)
                file_for_writer.flush()
            if count % 1000 == 0:
                print("Processed", count, "row")


def split_ld_meo_api_data():
    open_file = "../data_ld/api/meo_2018-04-01-00_2018-04-22-00.csv"
    with open(open_file, "r") as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        count = -1

        for row in reader:
            count += 1
            if count is 0:
                header_row = row
                continue
            with open("../data_ld/meo/" + row[0] + ".csv", "a", newline='') as file_for_writer:
                writer = csv.writer(file_for_writer, delimiter=',')
                row_to_write = [row[1]] + row[3:]
                writer.writerow(row_to_write)
                file_for_writer.flush()
            if count % 10000 == 0:
                print("Processed", count, "row")


# def api_meo_data_insert():
#     start, end = "2018-04-01 00:00:00", "2018-04-22 00:00:00"
#     with open("../data/api/meo_2018-04-01-00_2018-04-22-00.csv", "r") as csv_file:
#         rows = np.array(list(csv.reader(csv_file, delimiter=',')))
#         for grid_id in bj_raw_fetch.grid_location.keys():
#             grid_dict = dict()
#             for row in rows[rows[:, 0] == grid_id]:
#                 try:
#                     grid_dict[row[1]] = list(map(float_m, row[3:]))
#                 except ValueError:
#                     pass
#             with open("../data/meo/" + grid_id + ".csv", "a", newline='') as append_file:
#                 writer = csv.writer()


def format_ld_dt_string(dt_o):
    dt_s = "{}/{}/{} {}:00".format(dt_o.year, dt_o.month, dt_o.day, dt_o.hour)
    # dt_s = dt_o.strftime(format_string_m)
    return dt_s


def csv_ld_fill():
    aq_location, aq_dicts = ld_raw_fetch.load_aq_original()
    for aq_name in aq_location:
        aq_dict = aq_dicts[aq_name]
        start_dt_o, end_dt_o = datetime.strptime(list(aq_dict.keys())[0], format_string), \
                               datetime.strptime(list(aq_dict.keys())[-1], format_string)

        # Firstly fill lost time with all None value
        for dt_o in tools.per_delta(start_dt_o, end_dt_o, timedelta(hours=1)):
            # dt_s = dt_o.strftime(format_string)
            dt_s = format_ld_dt_string(dt_o)
            try:
                data = aq_dict[dt_s]
            except KeyError:
                aq_dict[dt_s] = [None] * 3

        start_dt_o += timedelta(hours=1)
        end_dt_o -= timedelta(hours=1)
        count = 0
        # Then fill data if only one row is lost
        for dt_o in tools.per_delta(start_dt_o, end_dt_o, timedelta(hours=1)):
            # dt_s = dt_o.strftime(format_string)
            dt_s = format_ld_dt_string(dt_o)
            data = aq_dict[dt_s]
            previous = aq_dict[format_ld_dt_string(dt_o - timedelta(hours=1))]
            following = aq_dict[format_ld_dt_string(dt_o + timedelta(hours=1))]
            for column in range(len(data)):
                if data[column] is None or data[column] < 0 or \
                        (column == 1 and data[column] > 200) or (column == 2 and data[column] > 300):
                    if previous[column] is not None and following[column] is not None:
                        data[column] = (previous[column] + following[column]) / 2
                        count += 1
                    else:
                        data[column] = None
            del aq_dict[dt_s]
            aq_dict[dt_s] = data
        print("Filled data in ", aq_name, ": ", count, sep='')

        # Write into csv
        with open("../data_ld_m/aq/" + aq_name + ".csv", "w", newline='') as file:
            writer = csv.writer(file, delimiter=',')
            for dt_s in aq_dict.keys():
                dt_s_m = datetime.strptime(dt_s, format_string).strftime(format_string_m)
                writer.writerow([dt_s_m] + aq_dict[dt_s])
            file.flush()


def csv_bj_fill():
    aq_location, aq_dicts = bj_raw_fetch.load_aq_original()
    for aq_name in aq_location:
        aq_dict = aq_dicts[aq_name]
        start_dt_o, end_dt_o = datetime.strptime(list(aq_dict.keys())[0], format_string_m), \
                               datetime.strptime(list(aq_dict.keys())[-1], format_string_m)

        # Firstly fill lost time with all None value
        for dt_o in tools.per_delta(start_dt_o, end_dt_o, timedelta(hours=1)):
            # dt_s = dt_o.strftime(format_string_m)
            dt_s = dt_o.strftime(format_string_m)
            try:
                data = aq_dict[dt_s]
            except KeyError:
                aq_dict[dt_s] = [None] * 6

        start_dt_o += timedelta(hours=1)
        end_dt_o -= timedelta(hours=1)
        count = 0
        # Then fill data if only one row is lost
        for dt_o in tools.per_delta(start_dt_o, end_dt_o, timedelta(hours=1)):
            # dt_s = dt_o.strftime(format_string_m)
            dt_s = dt_o.strftime(format_string_m)
            data = aq_dict[dt_s]
            previous = aq_dict[(dt_o - timedelta(hours=1)).strftime(format_string_m)]
            following = aq_dict[(dt_o + timedelta(hours=1)).strftime(format_string_m)]
            for column in range(len(data)):
                if data[column] is None:
                    if previous[column] is not None and following[column] is not None:
                        data[column] = (previous[column] + following[column]) / 2
                        count += 1
                    else:
                        continue
            del aq_dict[dt_s]
            aq_dict[dt_s] = data
        print("Filled data in ", aq_name, ": ", count, sep='')

        # Write into csv
        with open("../data_m/aq/" + aq_name + ".csv", "w", newline='') as file:
            writer = csv.writer(file, delimiter=',')
            for dt_s in aq_dict.keys():
                writer.writerow([dt_s] + aq_dict[dt_s])
                file.flush()


format_string_m = "%Y-%m-%d %H:%M:%S"
format_string = "%Y/%m/%d %H:%M"
# split_ld_meo_api_data()
# split_ld_aq_api_data()
# ld_forecast_aq_split()
# ld_other_aq_split()
csv_ld_fill()
# split_aq_api_data()
# split_meo_api_data()
