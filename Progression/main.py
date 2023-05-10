
from config_reader import ConfigurationReader
from controller_worker import ControllerWorker
from progression_table_processor import ProgressionTableProcessor

if __name__ == '__main__':
    config = ConfigurationReader()
    process_base = ProgressionTableProcessor(config.progression_sleep)
    controller = ControllerWorker(process_base)
    controller.start()

    process_base.start_process()




