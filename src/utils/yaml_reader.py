"""Module for reading YAML files into Python dictionaries."""

import yaml


def read_yaml_file(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)
