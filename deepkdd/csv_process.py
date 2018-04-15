import csv
from deepkdd import bj_raw_fetch, tools
from datetime import datetime, timedelta


def csv_split():
    with open("../data/London_historical_meo_grid.csv", "r") as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        count = 0
        for row in reader:
            if count is 0:
                header_row = row
            else:
                with open("../data_ld/meo/" + row[0] + ".csv", "a", newline='') as file_for_writer:
                    writer = csv.writer(file_for_writer, delimiter=',')
                    writer.writerow(row[3:])
                    file_for_writer.flush()
            count += 1
            if count % 10000 is 0:
                print("Already finished", count, "rows")


def csv_fill():
    aq_location, aq_dicts = bj_raw_fetch.load_aq()
    for aq_name in aq_location:
        aq_dict = aq_dicts[aq_name]
        start_dt_o, end_dt_o = datetime.strptime(list(aq_dict.keys())[0], format_string), \
                               datetime.strptime(list(aq_dict.keys())[-1], format_string)

        # Firstly fill lost time with all None value
        for dt_o in tools.per_delta(start_dt_o, end_dt_o, timedelta(hours=1)):
            dt_s = dt_o.strftime(format_string)
            try:
                data = aq_dict[dt_s]
            except KeyError:
                aq_dict[dt_s] = [None] * 6

        start_dt_o += timedelta(hours=1)
        end_dt_o -= timedelta(hours=1)
        count = 0
        # Then fill data if only one row is lost
        for dt_o in tools.per_delta(start_dt_o, end_dt_o, timedelta(hours=1)):
            dt_s = dt_o.strftime(format_string)
            data = aq_dict[dt_s]
            previous = aq_dict[(dt_o - timedelta(hours=1)).strftime(format_string)]
            following = aq_dict[(dt_o + timedelta(hours=1)).strftime(format_string)]
            for column in range(len(data)):
                if data[column] is None:
                    if previous[column] is not None and following[column] is not None:
                        data[column] = (previous[column] + following[column]) / 2
                        count += 1
            aq_dict[dt_s] = data
        print("Filled data in ", aq_name, ": ", count, sep='')

        # Write into csv
        with open("../data_m/aq/" + aq_name + ".csv", "w", newline='') as file:
            writer = csv.writer(file, delimiter=',')
            for dt_s in aq_dict.keys():
                writer.writerow([dt_s] + aq_dict[dt_s])
            file.flush()


format_string = "%Y-%m-%d %H:%M:%S"
# csv_fill()
csv_split()
