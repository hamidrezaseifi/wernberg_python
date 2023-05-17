import configparser
import os
from os.path import dirname
from datetime import datetime


class ConfigurationReader:
    db_host = None
    db_port = None
    db_user = None
    db_password = None
    db_database = None
    _sleep = None
    last_config_changed = None

    def __init__(self):

        self._db_conf_file = os.path.join(dirname(__file__), "config.ini")
        if not os.path.exists(self._db_conf_file):
            raise ValueError(f"Database config file '{self._db_conf_file}' not found!")

        self.reload_config()

    def reload_config(self):
        config = configparser.ConfigParser()
        config.read(self._db_conf_file)
        self.db_host = config['DATABASE']['host']
        self.db_port = config['DATABASE']['port']
        self.db_user = config['DATABASE']['user']
        self.db_password = config['DATABASE']['password']
        self.db_database = config['DATABASE']['database']
        self._sleep = int(config['PROGRESSION']['sleep'])
        self.last_config_changed = os.stat(self._db_conf_file).st_mtime

    def get_sleep(self) -> float:
        return self._sleep / 1000

    def is_config_changed(self):
        return self.last_config_changed != os.stat(self._db_conf_file).st_mtime

