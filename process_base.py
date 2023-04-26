import time
from abc import ABC, abstractmethod

from tab_1_processor import TableOneProcessor

class ProcessBase(ABC):
    _status: int = 1
    _tab_1: TableOneProcessor = None

    def __init__(self):
        self._tab_1 = TableOneProcessor()
        self.check_run_status()

    def intern_process(self):
        pass

    def start_process(self):
        while True:
            if self._status == 1:
                self.intern_process()
            else:
                print("Status is paused!")
            time.sleep(1)

    def check_run_status(self):
        self._tab_1.load_data()
        self._status = self._tab_1.get_int_data("9POW", "l1")
