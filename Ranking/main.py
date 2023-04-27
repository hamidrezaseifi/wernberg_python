# This is a sample Python script.
from Ranking.ranking_table_processor import RankingTableProcessor
from Ranking.tab_2_processor import TableTwoConnectionProcessor
from Ranking.tab_2b_processor import TableTwoBConnectionProcessor
from controller_worker import ControllerWorker
from tab_1_processor import ConnectionOneProcessor


# Press Umschalt+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.




if __name__ == '__main__':

    main = RankingTableProcessor()

    controller = ControllerWorker(main)
    controller.start()

    main.start_process()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
