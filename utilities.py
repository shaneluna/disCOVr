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

def write_json_pretty_to_file(json_string: str, filepath: str) -> None:
    """
    Write json to file in pretty format (indented).
    """
    with open(filepath, 'w', encoding="utf-8") as f:
        json.dump(json_string, f, ensure_ascii=False, indent=4)

def get_files(path: str, extension: str) -> list:
    """
    Return list of filepaths given search path and extension.
    """
    return glob.glob(f"{path}*.{extension}") # for full filepath

def create_dir(path: str) -> None:
    """
    Create new directory; if already exists do nothing.
    """
    already_exists = os.path.exists(path)
    # if no, create; else, do nothing
    if not already_exists: os.makedirs(path)