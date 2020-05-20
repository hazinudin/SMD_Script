import json
import os


class SMDConfigs(object):
    def __init__(self, config_file='smd_config.json'):
        """
        This class will load all the first level keys as class attributes.
        """
        smd_folder = self.smd_dir
        file_path = os.path.join(smd_folder, config_file)

        with open(file_path) as config_f:  # Open the config file in the root directory
            config_dict = json.load(config_f)

        for keys, value in config_dict.items():  # Assign dictionary keys to a class attribute
            setattr(self, keys, value)

    @property
    def smd_dir(self):
        """
        This class property return the SMD Pacakge directory
        :return: SMD folder path
        """
        module_folder = os.path.dirname(__file__)
        smd_folder = os.path.dirname(module_folder)

        return smd_folder

class Configs(object):
    """
    This class will load the first level keys as class attribtues, load config JSON file other than SMD config file.
    """
    def __init__(self, config_file):
        file_path = os.path.join(os.getcwd(), config_file)

        with open(file_path) as config_f:  # Open the config file in the root directory
            config_dict = json.load(config_f)

        for keys, value in config_dict.items():  # Assign dictionary keys to a class attribute
            setattr(self, keys, value)

