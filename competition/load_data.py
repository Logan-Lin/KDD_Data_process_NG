import os

import pandas as pd

data_header_dict = {"bj": {"aq": ["utc_time", "pm2.5", "pm10", "no2", "co", "o3", "so2"],
                      "meo": ["utc_time", "temperature", "pressure", "humidity", "wind_direction", "wind_speed"]},
               "ld": {"aq": ["utc_time", "pm2.5", "pm10", "no2"],
                      "meo": ["utc_time", "temperature", "pressure", "humidity", "wind_direction", "wind_speed"]}}

# api_data_directory.format(city, data_type) to get real directory
api_data_directory = "../competition/data_{}_api_m/{}"
filled_data_directory = {"bj": "../data_m/aq_filled", "ld": "../data_ld_m/aq_filled"}
history_data_directory = {"bj": {"aq": "../data_m/aq", "meo": "../data/meo"},
                          "ld": {"aq": "../data_ld/aq", "meo": "../data/meo"}}


def load_directory_data(directory, data_header=None, drop=None, export_none=False):
    """
    Fetching all data from directory, have the ability of recursive scanning.

    :param data_header: indicating the head of data frame you want to append.
    :param directory: str or list, representing the directory(s) you want to fetch from.
        Be sure to correspond to city and data type you give.
    :param drop: list, containing all the column name you want to drop. Can be set to none to drop nothing
    :param export_none: bool, indicating drop rows that contain empty data or not.
        Noted that this drop are proceed after the drop of columns.
    :return: dict, containing all the data.
    :except: FileNotFoundError if the given directory not exist.
    """
    # Fetch all csv files recursively in given directory.
    df_array = []
    print("Loading data from {}".format(directory))
    if not isinstance(directory, list):
        directory = [directory]
    for one_directory in directory:
        for root, directories, filenames in os.walk(one_directory):
            for filename in filenames:
                name, ext = os.path.splitext(filename)
                if ext == ".csv":
                    file_dir = os.path.join(root, filename)
                    df_single = pd.read_csv(file_dir, "w", delimiter=",", names=data_header)
                    date_index = pd.to_datetime(df_single["utc_time"])
                    df_single["id"] = name
                    df_single = df_single.set_index(["id"])
                    df_single = df_single.drop(["utc_time"], axis=1)
                    df_single = df_single.set_index([date_index], append=True)
                    df_array.append(df_single)

    # Assign index and proceed drop
    df = pd.concat(df_array)
    df = df.sort_index()
    if drop is not None:
        df = df.drop(drop, axis=1)
    if not export_none:
        df = df.dropna(axis=0, how='any')
    df = df[~df.index.duplicated(keep='last')]
    return df
