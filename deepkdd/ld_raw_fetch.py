import csv
from datetime import datetime


def float_m(value):
    if value is None or len(value) == 0:
        # raise (ValueError("Data loss here"))
        # return None
        return -1.0
    return float(value)


# Load all needed data into dicts and lists.
# Load aq station location dict
aq_location = dict()
with open("../data/London_AirQuality_Stations.csv", "r") as read_file:
    reader = csv.reader(read_file, delimiter=',')
    for row in reader:
        try:
            if len(row[2]) == 0:
                continue
            aq_location[row[0]] = list(map(float_m, [row[5], row[4]]))
        except ValueError:
            pass


# Load grid location dict
grid_location = dict()
with open("../data/London_grid_location.csv", "r") as read_file:
    reader = csv.reader(read_file, delimiter=',')
    for row in reader:
        try:
            grid_location[row[0]] = list(map(float_m, row[1:]))
        except ValueError:
            pass


# Load aq station info dict
def load_aq_dicts():
    aq_dicts = dict()
    print("Loading aq data...")
    for aq_name in aq_location.keys():
        # loss_count = 0
        aq_dict = dict()
        with open("../data_ld_m/aq/" + aq_name + ".csv", "r") as aq_file:
            reader = csv.reader(aq_file, delimiter=',')
            for row in reader:
                try:
                    aq_dict[row[0]] = list(map(float_m, row[1:]))
                except ValueError:
                    # loss_count += 1
                    pass
        # print(aq_name, "loss", loss_count)
        aq_dicts[aq_name] = aq_dict
    return aq_dicts


# Load grid meo info dict
def load_grid_dicts():
    grid_dicts = dict()
    loaded = 0
    print("Loading grid meo data...")
    for grid_name in grid_location.keys():
        grid_dict = dict()
        with open("../data_ld/meo/" + grid_name + ".csv", "r") as grid_file:
            reader = csv.reader(grid_file, delimiter=',')
            for row in reader:
                try:
                    grid_dict[row[0]] = list(map(float_m, row[1:]))
                except ValueError:
                    pass
        grid_dicts[grid_name] = grid_dict
        loaded += 1
        if loaded % 20 is 0:
            print("Meo loaded %3.0f%%" % (loaded / len(grid_location.keys()) * 100))
    return grid_dicts


def load_all():
    return aq_location, grid_location, load_aq_dicts(), load_grid_dicts()


def load_aq():
    return aq_location, load_aq_dicts()


def load_grid():
    return grid_location, load_grid_dicts()


def load_location():
    return aq_location, grid_location
