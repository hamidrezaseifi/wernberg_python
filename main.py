import sys

from config_reader import ConfigurationReader
from controller_worker import ControllerWorker
from process_base import ProcessBase
from progression_table_processor import ProgressionTableProcessor
from ranking_table_processor import RankingTableProcessor

if __name__ == '__main__':
    config = ConfigurationReader()

    if len(sys.argv) > 1:
        start_module = sys.argv[1]
        process_base = None
        if start_module.lower() == "progression":
            process_base = ProgressionTableProcessor(config.progression_sleep)
        if start_module.lower() == "ranking":
            process_base = RankingTableProcessor(config.progression_sleep)

        if process_base is None:
            raise ValueError(f"Invalid start module! No start module is selected. valid values are ['ranking', 'progression']:\n main.py <start module>")

        controller = ControllerWorker(process_base)
        controller.start()

        process_base.start_process()
    else:
        raise ValueError(f"Invalid start module! No start module is selected. valid values are ['ranking', 'progression']:\n main.py <start module>")




