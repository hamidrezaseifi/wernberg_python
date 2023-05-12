import configparser
import os
from os.path import dirname
from datetime import datetime

class ConfigurationReader:
    latest_date = datetime.now()

    def __init__(self):
        db_conf_file = os.path.join(dirname(__file__), "config.ini")
        if not os.path.exists(db_conf_file):
            raise ValueError(f"Database config file '{db_conf_file}' not found!")

        config = configparser.ConfigParser()
        config.read(db_conf_file)

        self.db_host = config['DATABASE']['host']
        self.db_port = config['DATABASE']['port']
        self.db_user = config['DATABASE']['user']
        self.db_password = config['DATABASE']['password']
        self.db_database = config['DATABASE']['database']

        self._sleep = int(config['RANKING']['sleep'])

    def get_sleep(self) -> float:
        return self._sleep / 1000


CONFIG_READER = ConfigurationReader()
