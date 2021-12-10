import yaml
import json
import glob
import os

def read_yaml_config(filename: str) -> dict:
    """
    Return dictionary of parsed yaml config file.
    """
    with open(filename, "r") as yamlfile:
        config = yaml.safe_load(yamlfile)
    return config

def read_json_file(filepath: str) -> dict:
    """
    Return dictionary from json.
    """
    with open(filepath) as json_file:
        json_dict = json.load(json_file)
    return json_dict

def get_files(path: str, extension: str) -> list:
    """
    Return list of filepaths given search path and extension.
    """
    return glob.glob(f"{path}*.{extension}") # for full filepath