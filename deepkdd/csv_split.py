import csv


with open("../data/beijing_201802_201803_aq.csv", "r") as csv_file:
    reader = csv.reader(csv_file, delimiter=',')
    count = 0
    for row in reader:
        if count is 0:
            header_row = row
        else:
            with open("../data/aq/" + row[0] + ".csv", "a", newline='') as file_for_writer:
                writer = csv.writer(file_for_writer, delimiter=',')
                writer.writerow(row[1:])
                file_for_writer.flush()
        count += 1
        if count % 10000 is 0:
            print("Already finished", count, "rows")
