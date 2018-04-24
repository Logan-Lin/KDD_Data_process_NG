import os
import time as ti
from datetime import datetime, timedelta

import h5py
import numpy as np
from progressbar import ProgressBar as PB, Bar, Percentage

from utils.bj_raw_fetch import load_all_aq
from utils.tools import per_delta


def check_valid(aq_name, start_object):
    try:
        aq_dict = aq_dicts[aq_name]

        predict_matrix = []
        for i in range(1, predict_span + 1):
            predict_matrix.append([aq_dict[(start_object + timedelta(hours=i)).strftime(format_string)]
                                   [column] for column in [0, 1, 4]])
    except KeyError:
        return None
    return predict_matrix


if __name__ == '__main__':
    aq_location, aq_dicts = load_all_aq()
    # aq_dicts_no_pm10 = load_aq_pm10_dicts()
    aq_dicts_no_pm10 = aq_dicts
    format_string = "%Y-%m-%d %H:%M:%S"
    date_format_string = "%Y_%m_%d"
    start_datetime, end_datetime = datetime.strptime("2018-04-01 21:00:00", format_string), \
                                   datetime.strptime("2018-04-22 23:00:00", format_string)
    diff = end_datetime - start_datetime
    days, seconds = diff.days, diff.seconds
    delta_time = int(days * 24 + seconds // 3600)
    time_span = 0
    predict_span = 50

    grid_circ = 7
    data_dir = "../data/h5_predict"

    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    aq_count = 0
    for aq_name in aq_location.keys():
        aggregate = 0

        ti.sleep(0.1)
        bar = PB(initial_value=0, maxval=delta_time + 1,
                 widgets=[aq_name, ' ', Bar('=', '[', ']'), ' ', Percentage()])

        valid_count = 0

        # Validate the near grid matrix algorithm
        # plt.figure()
        # plt.title(aq_name)
        # plt.plot(aq_location[aq_name][0], aq_location[aq_name][1], '.')
        # plt.plot(grid_coor_array[:, 0], grid_coor_array[:, 1], '.')
        # plt.show()

        # Exporting data from start to end
        predict_matrix = []
        dt_int_array = []
        for dt_object_day in per_delta(start_datetime, end_datetime, timedelta(hours=24)):
            for dt_object in per_delta(dt_object_day, dt_object_day + timedelta(hours=2), timedelta(hours=1)):
                aggregate += 1
                bar.update(aggregate)
                dt_string = dt_object.strftime(format_string)

                # Fetch history and prediction data, check data validation in the same time
                predict = check_valid(aq_name, dt_object)
                if predict is None:
                    continue

                # Append this hour's data into per-day data
                predict_matrix.append(predict)
                dt_int_array.append(int(ti.mktime(dt_object.timetuple())))
                valid_count += 1

        h5_file = h5py.File("".join([data_dir, "/", aq_name, ".h5"]), "w")
        # h5_file.create_dataset("history", data=np.asarray(history_matrix))
        h5_file.create_dataset("predict", data=np.asarray(predict_matrix))
        h5_file.create_dataset("timestep", data=np.asarray(dt_int_array))
        h5_file.flush()
        h5_file.close()
        aq_count += 1
        print(" - valid%6.2f%%(%5d)" % ((100 * valid_count / aggregate), valid_count))
