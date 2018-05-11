import os
from datetime import datetime as dt, timedelta as td

import numpy as np
import pandas as pd

from competition import load_data
from utils import bj_raw_fetch, ld_raw_fetch

format_string = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d-%H", "%Y%m%d"]


true_df_dict = {"ld": load_data.load_directory_data("../competition/data_ld_api/aq",
                                                 data_header=load_data.data_header_dict["bj"]["aq"],
                                                 export_none=True),
                "bj": load_data.load_directory_data("../competition/data_bj_api/aq",
                                                 data_header=load_data.data_header_dict["bj"]["aq"],
                                                 export_none=True)}


def smape(actual, predicted):
    a = np.abs(actual - predicted)
    b = actual + predicted
    return 2 * np.mean(np.divide(a, b, out=np.zeros_like(a), where=b != 0, casting='unsafe'))


def cal_smape(predict_dt, file_suffix=None, predict_csv=None, city="bj", export_compare=False):
    predict_column = {"bj": ["PM2.5", "PM10", "O3"], "ld": ["PM2.5", "PM10"]}[city]
    real_column = {"bj": ["pm2.5", "pm10", "o3"], "ld": ["pm2.5", "pm10"]}[city]

    print("Evaluating on {} data".format(predict_dt))
    predict_dt = dt.strptime(predict_dt, format_string[1])
    if predict_csv is None:
        predict_df = pd.read_csv("../submit/sample_submission_{}.csv".format(predict_dt.strftime(format_string[2])),
                                 delimiter=",")
    else:
        predict_df = pd.read_csv(predict_csv, delimiter=",")
    t = pd.Series(predict_df["test_id"]).apply(lambda x: x.split('#')[0])
    t.index = predict_df.index.copy()
    predict_df['prefix'] = t

    # true_df = load_data.load_directory_data("../competition/data_{}_api/aq".format(city),
    #                                              data_header=load_data.data_header_dict["bj"]["aq"],
    #                                              export_none=True)
    true_df = true_df_dict[city]
    aq_dfs = []
    aq_names = {"bj": bj_raw_fetch.aq_location.keys(), "ld": ld_raw_fetch.aq_location.keys()}[city]
    for aq_name in aq_names:
        true_span_df = true_df.loc[aq_name, :].loc[predict_dt:predict_dt + td(hours=47), real_column]
        true_span_df = true_span_df.reset_index()
        predict_span_df = predict_df[predict_df["prefix"] == aq_name]
        predict_span_df = predict_span_df.reset_index()
        merge_df = pd.concat([predict_span_df, true_span_df], axis=1)
        aq_dfs.append(merge_df)
    concat_df = pd.concat(aq_dfs).reset_index()
    if city == "ld":
        concat_df = concat_df.drop(["O3"], axis=1)

    csv_directory = os.path.join("compare", predict_dt.strftime(format_string[2]))
    file_name = city + "_"
    if file_suffix is not None:
        file_name += file_suffix
    if export_compare:
        if not os.path.exists(csv_directory):
            os.makedirs(csv_directory)
        concat_df.to_csv(os.path.join(csv_directory, file_name + ".csv"), sep=",")

    concat_df = concat_df.dropna(how="any", axis=0)

    column_smapes = []
    for i in range(len(predict_column)):
        all_predict = np.array(concat_df.loc[:, predict_column[i]])
        all_real = np.array(concat_df.loc[:, real_column[i]])

        smape_value = smape(all_real, all_predict)
        column_smapes.append(smape_value)
        print("{}\t{}\t{}".format(city, predict_column[i], smape_value))

    all_predict = np.array(concat_df.loc[:, predict_column])
    all_real = np.array(concat_df.loc[:, real_column])
    smape_value = smape(all_real, all_predict)
    column_smapes.append(smape_value)
    print("{}\tAgg\t{}".format(city, smape_value))

    return [file_name] + column_smapes


if __name__ == "__main__":
    table = []
    for root, directories, filenames in os.walk("../submit"):
        for filename in filenames:
            name, ext = os.path.splitext(filename)
            if ext == ".csv":
                print(name)
                file_dir = os.path.join(root, filename)
                bj_smape = cal_smape(
                    predict_dt=dt.strptime((name.split("_"))[0], format_string[2]).strftime(format_string[1]),
                    predict_csv=file_dir, file_suffix=name, city="bj", export_compare=True)
                ld_smape = cal_smape(
                    predict_dt=dt.strptime((name.split("_"))[0], format_string[2]).strftime(format_string[1]),
                    predict_csv=file_dir, file_suffix=name, city="ld", export_compare=False)
                table.append(bj_smape + ld_smape[1:])
                # table.append(bj_smape)
                # table.append(ld_smape)
                print()
    columns = ["file_name", "pm2.5_bj", "pm10_bj", "o3_bj",
               "aggregate_bj", "pm2.5_ld", "pm10_ld", "aggregate_ld"]
    # columns = ["file_name", "pm2.5_bj", "pm10_bj", "o3_bj", "aggregate_bj"]
    # columns = ["file_name", "pm2.5_ld", "pm10_ld", "aggregate_ld"]
    df = pd.DataFrame(table, columns=columns)
    df = df.set_index("file_name")
    df = df.sort_index()
    df.to_csv("compare/compare_h48.csv", sep=",")
