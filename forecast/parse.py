# -*- coding:utf-8 -*-
import json


def changetime(dt_object):
    from datetime import datetime
    import time as ti
    format_string = "%Y-%m-%d %H:%M:%S"
    dt_object = datetime.strptime(dt_object, format_string)
    return int(ti.mktime(dt_object.timetuple()))


def directory(d):
    #     东 南 西 北 东南 东北 西南 西北 东南偏东 东南偏南 东北偏东 东北偏北 西南偏南 西南偏西 西北偏西 西北偏北
    dir = ['东', '南', '西', '北', '东南', '东北', '西南', '西北', '东南偏东', '东南偏南', '东北偏东', '东北偏北', '西南偏南', '西南偏西', '西北偏西', '西北偏北']
    index = dir.index(d)
    return index


def dironehot(d):
    arr = []
    for i in range(0, 16):
        arr.append(0)
    arr[d] = 1
    return arr


def get_data(filename):
    output = []
    try:
        with open(filename, 'r') as f:
            data = [json.loads(i.strip()) for i in f.readlines()]
            # print(len(data))
            for d in data:
                # print(len(d['data']))
                count = 0
                for row in d['data']:
                    if count == 0:
                        count += 1
                        continue
                    # print(count) {'温度 (°C)': '17°', 'RealFeel': '15°', '风速 (千米/时)': '18 东北', '雨': '0%', '雪': '0%',
                    # '冻雪': '0%', '紫外线指数': '0','云量': '45%', '湿度': '49%', '露点': '6°', '能见度': '16 公里', '时间': '2018-04-23
                    # 20:00:00'} print(row)
                    weather = []
                    row['时间'] = changetime(row['时间'])
                    # weather.append(changetime(d['time']))
                    weather.append(int(str.split(row['温度 (°C)'], '°')[0]))
                    # weather.append(int(str.split(row['RealFeel®'], '°')[0]))
                    # weather.append(int(str.split(row['雨'], '%')[0]))
                    # weather.append(int(str.split(row['雪'], '%')[0]))
                    # weather.append(int(str.split(row['冻雪'], '%')[0]))
                    # weather.append(int(row['紫外线指数']))
                    # weather.append(int(str.split(row['云量'], '%')[0]))
                    weather.append(int(str.split(row['湿度'], '%')[0]))
                    # weather.append(int(str.split(row['露点'], '°')[0]))
                    # weather.append(int(str.split(row['能见度'], ' 公里')[0]))
                    # weather.append(int(row['时间']))
                    weather.append(int(str.split(row['风速 (千米/时)'], ' ')[0]))
                    arr = dironehot(directory(str.split(row['风速 (千米/时)'], ' ')[1]))
                    for i in range(0, len(arr)):
                        weather.append(arr[i])
                    # print(weather)
                    count += 1
                    if count >= 52:
                        break
                    output.append(weather)
    except FileNotFoundError:
        raise FileNotFoundError("Forecast data not exist here")
    return output


if __name__ == "__main__":
    data = get_data("data/bj_04_30_22.txt")
    print()
