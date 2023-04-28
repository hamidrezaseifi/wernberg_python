from Ranking.ranking_table_processor import RankingTableProcessor
from controller_worker import ControllerWorker


if __name__ == '__main__':

    main = RankingTableProcessor()

    controller = ControllerWorker(main)
    controller.start()

    main.start_process()

