import matplotlib.pyplot as plt
import csv


with open("../data/meo/beijing_grid_000.csv") as csv_file:
    reader = csv.reader(csv_file, delimiter=',')
    data = []
    for row in reader:
        value = row[5]
        if len(value) > 0:
            data.append(float(value))
    x = range(len(data))
    data = sorted(data)
    plt.plot(x, data)
    plt.show()
