import configparser
import os
import sys
from os.path import dirname
import logging
import logging.config
from datetime import datetime

class ConfigurationReader:
    latest_date = datetime.now()

    def __init__(self):
        logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',
                            level=logging.DEBUG,
                            datefmt='%Y-%m-%d %I:%M:%S')

        self._config_logger()

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

        self.progression_sleep = int(config['PROGRESSION']['sleep'])

        self.ranking_sleep = int(config['RANKING']['sleep'])

    def _config_logger(self):

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)


CONFIG_READER = ConfigurationReader()
