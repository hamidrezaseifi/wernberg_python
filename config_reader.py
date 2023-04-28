import configparser
import os
import sys
from os.path import dirname
import logging
import logging.config
from datetime import datetime

class ConfigurationReader:
    last_date = datetime.now()

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

        log_folder = os.path.join(dirname(__file__), "log")
        if not os.path.exists(log_folder):
            os.mkdir(log_folder)
        log_folder = os.path.join(log_folder, "{:%Y-%m}".format(ConfigurationReader.last_date))
        if not os.path.exists(log_folder):
            os.mkdir(log_folder)

        log_file = os.path.join(log_folder, self._extract_log_file_name(ConfigurationReader.last_date))

        if len(logger.handlers) < 2:

            logger.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s (%(levelname)-s) %(filename)s fn:%(funcName)s line(%(lineno)d) | %(message)s', datefmt="%Y/%m-%d %H:%M:%S")

            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)

            logger.addHandler(file_handler)
        else:
            if self._extract_log_file_name(ConfigurationReader.last_date) != self._extract_log_file_name(datetime.now()):
                ConfigurationReader.last_date = datetime.now()
                log_file = os.path.join(log_folder, self._extract_log_file_name(ConfigurationReader.last_date))
                file_handler = logger.handlers[0]
                file_handler.baseFilename = log_file

    @staticmethod
    def _extract_log_file_name(dt: datetime):
        return "{:%Y-%m-%d}.log".format(dt)
