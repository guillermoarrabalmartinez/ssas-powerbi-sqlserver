import json

def get_config():
    """Function to load the configuration from config.json"""
    with open('configuration\config.json', 'r') as config_file:
        config = json.load(config_file)
    return config
