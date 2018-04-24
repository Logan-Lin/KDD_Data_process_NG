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
    angle = angle % 360
    angle_split_array = [11.25]
    while True:
        append_value = angle_split_array[-1] + 22.5
        if append_value > 360:
            break
        angle_split_array.append(append_value)
    index = 0
    sequence = [3, 11, 5, 10, 0, 8, 4, 9, 1, 12, 6, 13, 2, 14, 7, 15]
    while True:
        if angle_split_array[0] >= angle or angle_split_array[-1] < angle:
            index = 0
            break
        if angle_split_array[index - 1] < angle <= angle_split_array[index]:
            break
        index += 1
    return sequence[index]


def get_one_hot(value, aggregate):
    one_hot_array = [0] * aggregate
    one_hot_array[value] = 1
    return one_hot_array


if __name__ == '__main__':
    for i in range(0, 361):
        print(i, get_one_hot(angle_to_int(i), 16))
