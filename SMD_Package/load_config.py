import os
import json


class Configs(object):
    def __init__(self, config_file='smd_config.json'):
        """
        This class will load all the first level keys as class attributes.
        """
        file_path = os.path.join(os.getcwd(), config_file)

        with open(file_path) as config_f:  # Open the config file in the root directory
            config_dict = json.load(config_f)

        for keys, value in config_dict.items():  # Assign dictionary keys to a class attribute
            setattr(self, keys, value)
