from os import listdir
from os.path import isfile, join


def remove_empty_line(file_name):
    with open(file_name, "r") as old_file:
        old_lines = old_file.readlines()
    with open(file_name, "w") as new_file:
        for line in old_lines:
            line = line[:-1]
            if len(line) > 0:
                new_file.write(line + "\n")
                new_file.flush()
    print("Finished processing", file_name)


file_path = "../DeepLearningModel/"
only_py = [join(file_path, f) for f in listdir(file_path) if isfile(join(file_path, f)) and f.split(".")[-1] == 'py']
for py_file in only_py:
    remove_empty_line(py_file)
