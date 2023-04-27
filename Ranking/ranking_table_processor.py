import time
from operator import itemgetter

from Ranking.tab_2_processor import TableTwoConnectionProcessor
from process_base import ProcessBase
from tab_1_processor import ConnectionOneProcessor


class RankingTableProcessor(ProcessBase):
    _tables = []
    _selected_table_index = -1
    _selected_table_name = ""
    _tab_2: TableTwoConnectionProcessor = None
    _last_serie_index: int = 27
    _tab_1: ConnectionOneProcessor = None
    _status: int = 1
    _last_proceed_id: str = None

    def __init__(self):
        super().__init__()
        self._tab_1 = ConnectionOneProcessor()
        self._tab_2 = TableTwoConnectionProcessor()
        self.check_run_status()

    def process_next_data(self):
        print("Start processing tables ...")

        self._tab_1.load_data()
        min_value = self._tab_1.get_int_data("3DIF", "l1")
        cutoff_value = self._tab_1.get_int_data("4CUT", "l1")

        # 1. Schritt
        row_list = self._tab_2.read_table_data()

        not_proceed_list = row_list
        if self._last_proceed_id:
            not_proceed_list = []
            start_to_select = False
            for i in range(0, len(row_list)):
                if row_list[i][0] == self._last_proceed_id:
                    start_to_select = True
                    continue
                if start_to_select:
                    not_proceed_list.append(row_list[i])

        not_proceed_list = not_proceed_list[:10:1]

        print(not_proceed_list)

        if len(not_proceed_list) > 0:
            self._last_proceed_id = not_proceed_list[len(not_proceed_list) - 1][0]

        # 2. Schritt
        move1_index = self._tab_2.get_column_index("move1")
        move2_index = self._tab_2.get_column_index("move2")

        not_proceed_list = [r for r in not_proceed_list if r[move1_index] is not None and r[move2_index]]
        not_proceed_list = [r for r in not_proceed_list if int(r[move1_index]) > 0]
        #not_proceed_list = [r for r in not_proceed_list if int(r[move2_index]) > 0]
        not_proceed_list = [r for r in not_proceed_list if int(r[move1_index]) - int(r[move2_index]) > min_value]

        print(not_proceed_list)

        # 3. Schritt

        sorted_not_proceed_list = sorted(not_proceed_list, key=itemgetter(move1_index), reverse=True)

        # 4. Schritt

        if cutoff_value is not None and cutoff_value > 0:
            sorted_not_proceed_list =  sorted_not_proceed_list[:cutoff_value:1]

        # 5. Schritt

        vol1_index = self._tab_2.get_column_index("vol1")
        vol2_index = self._tab_2.get_column_index("vol2")
        ls_index = self._tab_2.get_column_index("ls")

        for row in sorted_not_proceed_list:
            vol1 = int(row[vol1_index])
            vol2 = int(row[vol2_index])
            ls = int(row[ls_index])
            if ls == 1:
                row.append(vol1/vol2)
            else:
                if ls == 2:
                    row.append(vol2 / vol1)
                else:
                    row.append(0)

        print(sorted_not_proceed_list)

        # 6. Schritt


    def intern_process(self):
        while True:
            if self._status == 1:
                self.process_next_data()
            else:
                print("Status is paused!")
            time.sleep(1)

    def check_run_status(self):
        self._tab_1.load_data()
        self._status = self._tab_1.get_int_data("9POW", "l1")


