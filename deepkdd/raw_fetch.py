import csv


def float_m(value):
    if value is None or len(value) == 0:
        raise(ValueError("Data loss here"))
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
aq_dicts = dict()
print("Loading aq data...")
for aq_name in aq_location.keys():
    aq_dict = dict()
    with open("../data/aq/" + aq_name + ".csv") as aq_file:
        reader = csv.reader(aq_file, delimiter=',')
        for row in reader:
            try:
                aq_dict[row[0]] = list(map(float_m, row[1:]))
            except ValueError:
                pass
    aq_dicts[aq_name] = aq_dict

# Load grid meo info dict
grid_dicts = dict()
loaded = 0
print("Loading grid meo data...")
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
    if loaded % 20 is 0:
        print("Meo loaded %3.0f%%" % (loaded / len(grid_location.keys()) * 100))


def load_all():
    return aq_location, grid_location, aq_dicts, grid_dicts
