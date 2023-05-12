import logging
import time
from abc import ABC, abstractmethod

from config_reader import CONFIG_READER
from tab_1_processor import ConnectionOneProcessor

class ProcessBase(ABC):
    _status: int = 1
    _sleep: int = 1
    _tab_1: ConnectionOneProcessor = None

    def __init__(self):
        self._sleep = CONFIG_READER.get_sleep()
        self._tab_1 = ConnectionOneProcessor()
        self._check_run_status()

    def intern_process(self):
        pass

    def start_process(self):
        while True:
            self._check_run_status()
            if self._status == 1:
                self.intern_process()
            else:
                logging.info("Status is paused!")
            time.sleep(self._sleep)

    def _check_run_status(self):
        self._tab_1.load_data()
        self._status = self._tab_1.get_int_data("9POW", "l1")

