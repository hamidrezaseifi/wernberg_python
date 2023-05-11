
from config_reader import CONFIG_READER
from controller_worker import ControllerWorker
from ranking_table_processor import RankingTableProcessor

if __name__ == '__main__':

    process_base = RankingTableProcessor(CONFIG_READER.progression_sleep)

    controller = ControllerWorker(process_base)
    controller.start()

    process_base.start_process()




