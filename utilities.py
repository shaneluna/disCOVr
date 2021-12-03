import yaml

def read_yaml_config(filename: str) -> dict:
    with open(filename, "r") as yamlfile:
        config = yaml.safe_load(yamlfile)
    return config