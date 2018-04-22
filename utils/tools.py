import math


def per_delta(start, end, delta):
    curr = start
    while curr <= end:
        yield curr
        curr += delta


def cal_dis(coor1, coor2):
    dist = math.sqrt(math.pow(float(coor1[0]) - float(coor2[0]), 2) +
                     (math.pow(float(coor1[1]) - float(coor2[1]), 2)))
    return dist
