import csv
from progressbar import ProgressBar as PB, Bar, Percentage
from time import sleep


def float_m(value):
    if value is None or len(value) == 0:
        raise (ValueError("Data loss here"))
        # return None
    return float(value)


# Load all needed data into dicts and lists.
# Load aq station location dict
aq_location = dict()
with open("../data/Beijing_AirQuality_Stations_cn.csv") as read_file:
    reader = csv.reader(read_file, delimiter='\t')
    for row in reader:
        try:
            aq_location[row[0]] = list(map(float_m, row[1:]))
        except ValueError:
            pass


# Load grid location dict
grid_location = dict()
grid_location_r = dict()
with open("../data/beijing_grid_location.csv") as read_file:
    reader = csv.reader(read_file, delimiter=',')
    for row in reader:
        try:
            grid_location[row[0]] = list(map(float_m, row[1:3]))
        except ValueError:
            pass


# Load aq station info dict
def load_aq_dicts():
    aq_dicts = dict()
    print("Loading aq data...")
    for aq_name in aq_location.keys():
        aq_dict = dict()
        with open("../data_m/aq/" + aq_name + ".csv") as aq_file:
            reader = csv.reader(aq_file, delimiter=',')
            for row in reader:
                try:
                    aq_dict[row[0]] = list(map(float_m, row[1:]))
                except ValueError:
                    pass
        aq_dicts[aq_name] = aq_dict
    return aq_dicts


# Load grid meo info dict
def load_grid_dicts():
    grid_dicts = dict()
    loaded = 0
    print("Loading grid meo data...")

    bar = PB(initial_value=0, maxval=len(grid_location.keys()), widgets=['Grid load ', Bar('=', '[', ']'), ' ', Percentage()])
    for grid_name in grid_location.keys():
        grid_dict = dict()
        with open("../data/meo/" + grid_name + ".csv") as grid_file:
            reader = csv.reader(grid_file, delimiter=',')
            for row in reader:
                try:
                    grid_dict[row[0]] = list(map(float_m, row[1:]))
                except ValueError:
                    pass
        grid_dicts[grid_name] = grid_dict
        loaded += 1
        bar.update(loaded)

    sleep(0.1)
    return grid_dicts


def load_all():
    return aq_location, grid_location, load_aq_dicts(), load_grid_dicts()


def load_aq():
    return aq_location, load_aq_dicts()


def load_grid():
    return grid_location, load_grid_dicts()


def load_location():
    return aq_location, grid_location
