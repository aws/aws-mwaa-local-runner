import json
import os


def write_to_file(file_name, data):
    directory = os.path.dirname(file_name)
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(file_name, "a") as f:
        json.dump(data, f)


def read_from_file(file_name):
    with open(file_name, "r") as f:
        return json.load(f)
