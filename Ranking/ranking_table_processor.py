import logging
from operator import itemgetter

from tab_2_processor import TableTwoConnectionProcessor
from tab_3_processor import TableThreeConnectionProcessor
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
    _tab_3: TableThreeConnectionProcessor = None

    move1_index: int = -1
    move2_index: int = -1
    ls_index: int = -1
    id_index: int = -1
    vol1_index: int = -1
    vol2_index: int = -1


    def __init__(self, sleep_seconds: int):
        super().__init__(sleep_seconds)
        logging.info('Starting Ranking modules.')

        self._tab_1 = ConnectionOneProcessor()
        self._tab_2 = TableTwoConnectionProcessor()
        self._tab_3 = TableThreeConnectionProcessor()

        self.move1_index = self._tab_2.get_column_index("move1")
        self.move2_index = self._tab_2.get_column_index("move2")
        self.ls_index = self._tab_2.get_column_index("ls")
        self.id_index = self._tab_2.get_column_index("id")
        self.vol1_index = self._tab_2.get_column_index("vol1")
        self.vol2_index = self._tab_2.get_column_index("vol2")
        self.latest_index = self._tab_2.get_column_index("LATEST")

        self.check_run_status()

    def process_next_data(self):
        logging.info("Start processing rankings ...")

        cutoff_value, liq_value, min_value_dif, min_value_rat = self.read_base_data()

        if liq_value == 0:
            logging.info("There is 0 value in 6LIQ : L2!")
            return

        # 1. Schritt
        not_proceed_list = self.schritt_1()

        #not_proceed_list = not_proceed_list[:10:1]

        if len(not_proceed_list) > 0:
            self._last_proceed_id = not_proceed_list[len(not_proceed_list) - 1][self.id_index]
        else:
            logging.info("There is no new data to process in Tab_03")
            return

        # 2. Schritt

        not_proceed_list = self.schritt_2(min_value_dif, not_proceed_list)

        if len(not_proceed_list) == 0:
            logging.info("Results of step 2 is empty!")
            return

        # 3. Schritt

        sorted_not_proceed_list = self.schritt_3(not_proceed_list)

        if len(sorted_not_proceed_list) == 0:
            logging.info("Results of step 3 is empty!")
            return

        # 4. Schritt

        sorted_not_proceed_list = self.schritt_4(cutoff_value, sorted_not_proceed_list)

        if len(sorted_not_proceed_list) == 0:
            logging.info("Results of step 4 is empty!")
            return

        sorted_not_proceed_list = self.schritt_5_6_7(min_value_rat, sorted_not_proceed_list)

        if len(sorted_not_proceed_list) > 0:
            # 8. Schritt
            # 9. Schritt
            self.schritt_8_9(liq_value, sorted_not_proceed_list)
        else:
            logging.info("Results of steps 5_6_7 is empty!")

    def schritt_8_9(self, liq_value, sorted_not_proceed_list):
        first_row = sorted_not_proceed_list[0]
        vol1 = first_row[self.vol1_index]
        vol2 = first_row[self.vol2_index]
        latest = first_row[self.latest_index]
        bmax = (vol1 + vol2) * latest / liq_value
        data_to_inset = {"id": first_row[self.id_index], "ls": first_row[self.ls_index], "bmax": bmax}
        data_to_update = {"ls": first_row[self.ls_index], "bmax": bmax}
        #self._tab_3.insert_data_duplicate(data_to_inset, data_to_update)
        logging.info(f"Update tab_3: {data_to_inset}")
        self._tab_3.update(data_to_inset)

    def schritt_5_6_7(self, min_value_rat, in_sorted_not_proceed_list):
        if min_value_rat is not None and min_value_rat > 0:
            # 5. Schritt

            for row in in_sorted_not_proceed_list:
                vol1 = float(row[self.vol1_index])
                vol2 = float(row[self.vol2_index])
                ls = int(row[self.ls_index])
                if vol1 == 0 or vol2 == 0:
                    row.append(0)
                else:
                    if ls == 1:
                        row.append(vol1 / vol2)
                    else:
                        if ls == 2:
                            row.append(vol2 / vol1)
                        else:
                            row.append(0)

            # 6. Schritt

            ergebnis_index = len(in_sorted_not_proceed_list[0]) - 1

            in_sorted_not_proceed_list = [r for r in in_sorted_not_proceed_list if r[ergebnis_index] >= min_value_rat]

            # 7. Schritt

            in_sorted_not_proceed_list = sorted(in_sorted_not_proceed_list, key=itemgetter(ergebnis_index), reverse=True)

        return in_sorted_not_proceed_list

    def schritt_4(self, cutoff_value, sorted_not_proceed_list):
        if cutoff_value is not None and cutoff_value > 0:
            sorted_not_proceed_list = sorted_not_proceed_list[:cutoff_value:1]
        return sorted_not_proceed_list

    def schritt_3(self, not_proceed_list):
        sorted_not_proceed_list = sorted(not_proceed_list, key=itemgetter(self.move1_index), reverse=True)
        return sorted_not_proceed_list

    def schritt_2(self, min_value_dif, not_proceed_list):
        #logging.info(f"Filtering list in step 2: min_value_dif: {min_value_dif}")
        #print()
        #print(not_proceed_list)
        #print()

        #not_proceed_list = [r for r in not_proceed_list if r[self.move1_index] is not None and r[self.move2_index] is not None]
        #not_proceed_list = [r for r in not_proceed_list if float(r[self.move1_index]) > 0]
        # not_proceed_list = [r for r in not_proceed_list if float(r[self.move2_index]) > 0]

        for r in range(0, len(not_proceed_list)):
            for c in range(2, len(not_proceed_list[r])):
                if not_proceed_list[r][c] is None:
                    not_proceed_list[r][c] = 0

        not_proceed_list = [r for r in not_proceed_list if
                            float(r[self.move1_index]) - float(r[self.move2_index]) > min_value_dif]
        return not_proceed_list

    def schritt_1(self):
        row_list = self._tab_2.read_table_data()
        not_proceed_list = row_list
        if self._last_proceed_id:
            not_proceed_list = []
            start_to_select = False
            for i in range(0, len(row_list)):
                if row_list[i][self.id_index] == self._last_proceed_id:
                    start_to_select = True
                    continue
                if start_to_select:
                    not_proceed_list.append(row_list[i])
        return not_proceed_list

    def read_base_data(self):
        self._tab_1.load_data()
        min_value_dif = self._tab_1.get_float_data("3DIF", "l1")
        min_value_rat = self._tab_1.get_int_data("2RAT", "l2")
        cutoff_value = self._tab_1.get_int_data("4CUT", "l1")
        liq_value = self._tab_1.get_int_data("6LIQ", "l1")
        return cutoff_value, liq_value, min_value_dif, min_value_rat

    def intern_process(self):
        self.process_next_data()

