import os
from datetime import datetime as dt, timedelta as td

import numpy as np
import pandas as pd

from competition import load_data
from utils import bj_raw_fetch, ld_raw_fetch

format_string = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d-%H", "%Y%m%d"]


def smape(actual, predicted):
    a = np.abs(actual - predicted)
    b = actual + predicted
    return 2 * np.mean(np.divide(a, b, out=np.zeros_like(a), where=b != 0, casting='unsafe'))


def file_smape(predict_dt, file_suffix=None, predict_csv=None):
    """
    Calculate smape based on predict submission file and real data from api,
    output csv file for easy comparision at the same time.

    :param predict_dt: The start date of the predict
    :param file_suffix: The suffix you want to add to the output comparision file.
    :param predict_csv: Appoint for the predict submission file.
        If left None, will open default format submission file.
    """
    predict_dt = dt.strptime(predict_dt, format_string[1])
    if predict_csv is None:
        predict_df = pd.read_csv("../submit/sample_submission_{}.csv".format(predict_dt.strftime(format_string[2])),
                                 delimiter=",")
    else:
        predict_df = pd.read_csv(predict_csv, delimiter=",")
    t = pd.Series(predict_df["test_id"]).apply(lambda x: x.split('#')[0])
    t.index = predict_df.index.copy()
    predict_df['prefix'] = t

    split_index = predict_df[predict_df["test_id"] == "BL0#0"].index.values.astype(int)[0]
    bj_predict = predict_df[:split_index]
    ld_predict = predict_df[split_index:]
    ld_predict = ld_predict.drop("O3", axis=1)

    bj_true_data = load_data.load_directory_data(load_data.api_data_directory.format("bj", "aq"),
                                                 data_header=load_data.data_header_dict["bj"]["aq"],
                                                 export_none=True)
    ld_true_data = load_data.load_directory_data(load_data.api_data_directory.format("ld", "aq"),
                                                 data_header=load_data.data_header_dict["ld"]["aq"],
                                                 export_none=True)
    aq_dfs = []
    for aq_name in bj_raw_fetch.aq_location.keys():
        true_span_df = bj_true_data.loc[aq_name, :].loc[predict_dt:predict_dt + td(hours=47), ["pm2.5", "pm10", "o3"]]
        true_span_df = true_span_df.reset_index()
        predict_span_df = bj_predict[bj_predict["prefix"] == aq_name]
        predict_span_df = predict_span_df.reset_index()
        merge_df = pd.concat([predict_span_df, true_span_df], axis=1)
        aq_dfs.append(merge_df)
    bj_concat = pd.concat(aq_dfs).reset_index()

    aq_dfs = []
    for aq_name in ld_raw_fetch.aq_location.keys():
        true_span_df = ld_true_data.loc[aq_name, :].loc[predict_dt:predict_dt + td(hours=47), ["pm2.5", "pm10"]]
        true_span_df = true_span_df.reset_index()
        predict_span_df = ld_predict[ld_predict["prefix"] == aq_name]
        predict_span_df = predict_span_df.reset_index()
        merge_df = pd.concat([predict_span_df, true_span_df], axis=1)
        aq_dfs.append(merge_df)
    ld_concat = pd.concat(aq_dfs).reset_index()

    csv_directory = os.path.join("compare", predict_dt.strftime(format_string[2]))
    if file_suffix is not None:
        csv_directory = os.path.join(csv_directory, file_suffix)
    if not os.path.exists(csv_directory):
        os.makedirs(csv_directory)
    bj_concat.drop(["level_0", "index", "prefix"], axis=1).to_csv(os.path.join(csv_directory, "bj.csv"), sep=",")
    ld_concat.drop(["level_0", "index", "prefix"], axis=1).to_csv(os.path.join(csv_directory, "ld.csv"), sep=",")

    bj_concat = bj_concat.dropna(how="any", axis=0)
    ld_concat = ld_concat.dropna(how="any", axis=0)

    bj_all_predict = np.array(bj_concat.loc[:, ["PM2.5", "PM10", "O3"]])
    bj_all_real = np.array(bj_concat.loc[:, ["pm2.5", "pm10", "o3"]])

    ld_all_predict = np.array(ld_concat.loc[:, ["PM2.5", "PM10"]])
    ld_all_real = np.array(ld_concat.loc[:, ["pm2.5", "pm10"]])

    # all_predict = np.vstack([bj_all_predict.reshape(-1, 1), ld_all_predict.reshape(-1, 1)]).T
    # all_real = np.vstack([bj_all_real.reshape(-1, 1), ld_all_real.reshape(-1, 1)]).T
    # all_smape = smape(all_real, all_predict)
    # print("Average smape {}".format(all_smape))

    bj_size = np.shape(bj_all_predict)[0]
    ld_size = np.shape(ld_all_predict)[0]

    bj_smape = smape(bj_all_real, bj_all_predict)
    ld_smape = smape(ld_all_real, ld_all_predict)
    print("Beijing {} - {}".format(predict_dt, bj_smape))
    print("London {} - {}".format(predict_dt, ld_smape))
    print("Average {}".format((bj_smape + ld_smape) / 2))


if __name__ == "__main__":
    file_smape("2018-05-04-00")
