import csv
from utils import bj_raw_fetch, tools, ld_raw_fetch
from datetime import datetime, timedelta


def csv_split():
    with open("../data/London_historical_meo_grid.csv", "r") as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        count = 0
        for row in reader:
            if count is -1:
                header_row = row
            else:
                with open("../data/" + "London_grid_location" + ".csv", "a", newline='') as file_for_writer:
                    writer = csv.writer(file_for_writer, delimiter=',')
                    writer.writerow(row[:3])
                    file_for_writer.flush()
            count += 1
            if count > 1722:
                break
            if count % 10000 is 0:
                print("Already finished", count, "rows")


def format_london_dt_string(dt_o):
    dt_s = "{}/{}/{} {}:00".format(dt_o.year, dt_o.month, dt_o.day, dt_o.hour)
    return dt_s


def csv_fill():
    aq_location, aq_dicts = ld_raw_fetch.load_aq_original()
    for aq_name in aq_location:
        aq_dict = aq_dicts[aq_name]
        start_dt_o, end_dt_o = datetime.strptime(list(aq_dict.keys())[0], format_string), \
                               datetime.strptime(list(aq_dict.keys())[-1], format_string)

        # Firstly fill lost time with all None value
        for dt_o in tools.per_delta(start_dt_o, end_dt_o, timedelta(hours=1)):
            # dt_s = dt_o.strftime(format_string)
            dt_s = format_london_dt_string(dt_o)
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
            dt_s = format_london_dt_string(dt_o)
            data = aq_dict[dt_s]
            previous = aq_dict[format_london_dt_string(dt_o - timedelta(hours=1))]
            following = aq_dict[format_london_dt_string(dt_o + timedelta(hours=1))]
            for column in range(len(data)):
                if data[column] is None or data[column] < 0 or \
                        (column == 1 and data[column] > 200) or (column == 2 and data[column] > 300):
                    if previous[column] is not None and following[column] is not None:
                        data[column] = (previous[column] + following[column]) / 2
                        count += 1
                    else:
                        data[column] = None
            aq_dict[dt_s] = data
        print("Filled data in ", aq_name, ": ", count, sep='')

        # Write into csv
        with open("../data_ld_m/aq/" + aq_name + ".csv", "w", newline='') as file:
            writer = csv.writer(file, delimiter=',')
            for dt_s in aq_dict.keys():
                dt_s_m = datetime.strptime(dt_s, format_string).strftime(format_string_m)
                writer.writerow([dt_s_m] + aq_dict[dt_s])
            file.flush()


format_string_m = "%Y-%m-%d %H:%M:%S"
format_string = "%Y/%m/%d %H:%M"
csv_fill()
# csv_split()