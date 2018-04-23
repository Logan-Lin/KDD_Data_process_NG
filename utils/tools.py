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


def angle_to_int(angle):
    angle_split_array = [11.25]
    while True:
        append_value = angle_split_array[-1] + 22.5
        if append_value > 360:
            break
        angle_split_array.append(append_value)
    index = 0
    sequence = [4, 8, 12, 0, 6, 2, 10, 14]
    while True:
        if angle_split_array[0] > angle or angle_split_array[-1] < angle:
            index = 0
            break
        if angle_split_array[index - 1] < angle <= angle_split_array[index]:
            break
        index += 1
    return sequence[index]


if __name__ == '__main__':
    angle_to_int(100)
