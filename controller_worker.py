import logging
import time
from threading import Thread

from process_base import ProcessBase

class ControllerWorker(Thread):

    _table_processor: ProcessBase = None

    # time to check the status of work
    _sleep_seconds = 60

    def __init__(self, table_processor: ProcessBase):
        Thread.__init__(self)
        self._table_processor = table_processor

    def run(self):
        while True:
            logging.info("ControllerWorker: checking the run status ...")
            self._table_processor.check_run_status()
            time.sleep(self._sleep_seconds)
