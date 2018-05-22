from competition import load_data
import os
import pandas as pd
from datetime import datetime as dt
from utils import bj_raw_fetch


aq_df = load_data.load_directory_data(os.path.join("data_bj_api", "aq"),
                                      data_header=load_data.data_header_dict["bj"]["aq"],
                                      export_none=True)
format_string = "%Y-%m-%d-%H"
header = load_data.data_header_dict['bj']['aq'][1:]


def get_empty_rate(aq_name, start, end):
    """
    Count how many data are lost in each column.

    :param aq_name: str, air quality station name.
    :param start: str, start datetime string in "%Y-%m-%d-%H" format.
    :param end: str, end datetime string in "%Y-%m-%d-%H" format.
    """
    start, end = dt.strptime(start, format_string), dt.strptime(end, format_string)

    diff = end - start
    delta_time = int(diff.days * 24 + diff.seconds // 3600) + 1

    aq = aq_df.loc[aq_name, :][start:end]

    empty_counts = []

    for column in header:
        column_aq = aq[column]
        column_aq = column_aq.dropna(axis=0, how='any')
        valid_count = column_aq.shape[0]
        empty_counts.append("{}/{}".format(delta_time - valid_count, delta_time))

    return empty_counts


if __name__ == "__main__":
    start, end = "2018-05-20-22", "2018-05-21-22"

    count_matrix = []
    for aq_name in bj_raw_fetch.aq_location.keys():
        count_matrix.append(get_empty_rate(aq_name, start, end))

    count_df = pd.DataFrame(count_matrix, index=list(bj_raw_fetch.aq_location.keys()),
                            columns=header)
    print(count_df)
