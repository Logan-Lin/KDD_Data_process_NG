import csv
from progressbar import ProgressBar as PB, Bar, Percentage
from time import sleep
import operator

from utils.tools import cal_dis


def float_m(value):
    if value is None or len(value) == 0:
        raise (ValueError("Data loss here"))
        # return None
        # return -1.0
    return float(value)


def float_m_none(value):
    if value is None or len(value) == 0:
        return None
    return float(value)


# Load all needed data into dicts and lists.
# Load aq station location dict
aq_location = dict()
aq_location_all = dict()
with open("../data/London_AirQuality_Stations.csv", "r") as read_file:
    reader = csv.reader(read_file, delimiter=',')
    for row in reader:
        try:
            if len(row[2]) == 0:
                aq_location_all[row[0]] = list(map(float_m, [row[5], row[4]]))
                continue
                # pass
            aq_location_all[row[0]] = list(map(float_m, [row[5], row[4]]))
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
        loss_count = 0
        valid_count = 0
        aq_dict = dict()
        with open("../data_ld_m/aq/" + aq_name + ".csv", "r") as aq_file:
            reader = csv.reader(aq_file, delimiter=',')
            for row in reader:
                try:
                    aq_dict[row[0]] = list(map(float_m_none, row[1:3]))
                    valid_count += 1
                except ValueError:
                    loss_count += 1
                    pass
        # print(aq_name, " loss ", loss_count, ", loss ", 100 * (loss_count / (valid_count + loss_count)), sep='')
        aq_dicts[aq_name] = aq_dict
    return aq_dicts


def load_aq_modified_dicts():
    aq_dicts = load_aq_dicts()
    aq_modified_dicts = dict()
    print("Loading aq modified data...")
    for aq_name in aq_location.keys():
        aq_dict = dict()
        with open("../data_ld_m/aq/" + aq_name + ".csv", "r") as aq_file:
            reader = csv.reader(aq_file, delimiter=',')
            for row in reader:
                data_row = []
                try:
                    data_row += list(map(float_m, row[1:3]))
                except ValueError:
                    continue
                try:
                    data_row.append(float_m(row[3]))
                except ValueError:
                    try:
                        data_row.append(get_near_no2(aq_dicts, aq_name, row[0]))
                    except KeyError:
                        continue
                aq_dict[row[0]] = data_row
        aq_modified_dicts[aq_name] = aq_dict
    return aq_modified_dicts


def get_near_no2(aq_dicts, aq_name, dt_string):
    distance_matrix = get_distances(aq_name)
    valid = False
    result = 0
    for near_aq_name, distance in distance_matrix:
        try:
            result = aq_dicts[near_aq_name][dt_string][2]
            valid = True
            break
        except KeyError:
            valid = False
    if not valid:
        raise KeyError("Data fetching fail")
    return result


def load_aq_dicts_original():
    aq_dicts = dict()
    print("Loading aq data...")
    for aq_name in aq_location_all.keys():
        loss_count = 0
        valid_count = 0
        aq_dict = dict()
        with open("../data_ld/aq/" + aq_name + ".csv", "r") as aq_file:
            reader = csv.reader(aq_file, delimiter=',')
            for row in reader:
                try:
                    aq_dict[row[0]] = list(map(float_m, row[1:4]))
                    valid_count += 1
                except ValueError:
                    loss_count += 1
                    pass
        # print(aq_name, " loss ", loss_count, ", loss ", 100 * (loss_count / (valid_count + loss_count)), sep='')
        aq_dicts[aq_name] = aq_dict
    return aq_dicts


# Load grid meo info dict
def load_grid_dicts():
    grid_dicts = dict()
    loaded = 0

    bar = PB(initial_value=0, maxval=len(grid_location.keys()),
             widgets=['Grid load ', Bar('=', '[', ']'), ' ', Percentage()])

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
        bar.update(loaded)

    sleep(0.1)
    return grid_dicts


def get_distances(this_aq_name):
    distance_dict = dict()
    for aq_name in aq_location_all.keys():
        distance_dict[aq_name] = cal_dis(aq_location_all[this_aq_name], aq_location_all[aq_name])
    del distance_dict[this_aq_name]
    sorted_dis = sorted(distance_dict.items(), key=operator.itemgetter(1))
    return sorted_dis


def test_data_loss():
    print("Testing data loss...")
    test_column = [1, 2, 3]
    print("aq\tPM2.5\tPM10\tNO2")
    for aq_name in aq_location_all.keys():
        loss_counts = [0, 0, 0]
        aggregate = 0
        with open("../data_ld_m/aq/" + aq_name + ".csv", "r") as aq_file:
            reader = csv.reader(aq_file, delimiter=',')
            for row in reader:
                aggregate += 1
                for column in range(3):
                    try:
                        float_m(row[test_column[column]])
                    except ValueError:
                        loss_counts[column] += 1
            print(aq_name, end='')
            for loss_count in loss_counts:
                print("\t%4.f" % (100 * loss_count / aggregate), end='', sep='')
            if aq_name in list(aq_location.keys()):
                print("\tNeed Prediction", end='')
            print()


def load_all():
    return aq_location, grid_location, load_aq_dicts(), load_grid_dicts()


def load_aq():
    return aq_location, load_aq_dicts()


def load_grid():
    return grid_location, load_grid_dicts()


def load_location():
    return aq_location, grid_location


def load_aq_original():
    return aq_location, load_aq_dicts_original()

