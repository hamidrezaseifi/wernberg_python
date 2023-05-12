from operator import itemgetter

import time
from typing import List

from config_reader import ConfigurationReader

from mysql.connector import connect, Error


class RankingTableProcessor:
    _tab2_columns = ["id", "ls", "move1", "move2", "vol1", "vol2", "LATEST"]
    _tab2_name = "Tab_02"

    _tab3_name = "Tab_03"

    _last_serie_index: int = 27
    _status: int = 1
    _last_proceed_rows: List[str] = []

    move1_index: int = -1
    move2_index: int = -1
    ls_index: int = -1
    id_index: int = -1
    vol1_index: int = -1
    vol2_index: int = -1

    def __init__(self):

        self.connection = None

        self.move1_index = self.get_tab_2_column_index("move1")
        self.move2_index = self.get_tab_2_column_index("move2")
        self.ls_index = self.get_tab_2_column_index("ls")
        self.id_index = self.get_tab_2_column_index("id")
        self.vol1_index = self.get_tab_2_column_index("vol1")
        self.vol2_index = self.get_tab_2_column_index("vol2")
        self.latest_index = self.get_tab_2_column_index("LATEST")

        config = ConfigurationReader()
        self._sleep = config.get_sleep()

        self._db_host = config.db_host
        self._db_port = config.db_port
        self._db_user = config.db_user
        self._db_password = config.db_password
        self._db_database = config.db_database

    def process_next_data(self):
        print("Start processing rankings ...")

        cutoff_value, liq_value, min_value_dif, min_value_rat = self.read_base_data()

        if liq_value == 0:
            print("There is 0 value in 6LIQ : L2!")
            return

        # 1. Schritt
        not_proceed_list = self.schritt_1()

        if len(not_proceed_list) == 0:
            print("There is no new data to process in Tab_02")
            return

        # 2. Schritt

        if min_value_dif > 0:
            not_proceed_list = self.schritt_2(min_value_dif, not_proceed_list)

        if len(not_proceed_list) == 0:
            print("Results of step 2 is empty!")
            return

        # 3. Schritt

        sorted_not_proceed_list = self.schritt_3(not_proceed_list)

        if len(sorted_not_proceed_list) == 0:
            print("Results of step 3 is empty!")
            return

        # 4. Schritt

        sorted_not_proceed_list = self.schritt_4(cutoff_value, sorted_not_proceed_list)

        if len(sorted_not_proceed_list) == 0:
            print("Results of step 4 is empty!")
            return

        sorted_not_proceed_list = self.schritt_5_6_7(min_value_rat, sorted_not_proceed_list)

        if len(sorted_not_proceed_list) > 0:
            # 8. Schritt
            # 9. Schritt
            self.schritt_8_9(liq_value, sorted_not_proceed_list)
        else:
            print("Results of steps 5_6_7 is empty!")

    def schritt_8_9(self, liq_value, sorted_not_proceed_list):
        first_row = sorted_not_proceed_list[0]
        vol1 = first_row[self.vol1_index]
        vol2 = first_row[self.vol2_index]
        latest = first_row[self.latest_index]
        bmax = (vol1 + vol2) * latest / liq_value
        data_to_inset = {"id": first_row[self.id_index], "ls": first_row[self.ls_index], "bmax": bmax}
        print(f"Update tab_3: {data_to_inset}")
        self.update_tab_3(first_row[self.id_index], first_row[self.ls_index], bmax)

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

            in_sorted_not_proceed_list = sorted(in_sorted_not_proceed_list,
                                                key=itemgetter(ergebnis_index),
                                                reverse=True)

        return in_sorted_not_proceed_list

    def schritt_4(self, cutoff_value, sorted_not_proceed_list):
        if cutoff_value is not None and cutoff_value > 0:
            sorted_not_proceed_list = sorted_not_proceed_list[:cutoff_value:1]
        return sorted_not_proceed_list

    def schritt_3(self, not_proceed_list):
        sorted_not_proceed_list = sorted(not_proceed_list, key=itemgetter(self.move1_index), reverse=True)
        return sorted_not_proceed_list

    def schritt_2(self, min_value_dif, not_proceed_list):
        not_proceed_list = [r for r in not_proceed_list if
                            float(r[self.move1_index]) - float(r[self.move2_index]) > min_value_dif]
        return not_proceed_list

    def schritt_1(self):
        row_list = self.read_tab_2_data()
        not_proceed_list = []
        new_id_list = []

        for r in row_list:
            id_val = self._get_identity_vale(r)
            if id_val not in self._last_proceed_rows:
                not_proceed_list.append(r)
                new_id_list.append(id_val)

        if len(new_id_list) > 0:
            self._last_proceed_rows = new_id_list

        for r in range(0, len(not_proceed_list)):
            for c in range(2, len(not_proceed_list[r])):
                if not_proceed_list[r][c] is None:
                    not_proceed_list[r][c] = 0

        return not_proceed_list

    def read_base_data(self):
        values = self._read_tab1_value(["l1", "l2"], ["3DIF", "2RAT", "4CUT", "6LIQ"])

        min_value_dif = self.get_proper_float(values["3DIF"][0])
        min_value_rat = self.get_proper_float(values["2RAT"][1])
        cutoff_value = int(self.get_proper_float(values["4CUT"][0]))
        liq_value = self.get_proper_float(values["6LIQ"][0])

        return cutoff_value, liq_value, min_value_dif, min_value_rat

    def intern_process(self):
        try:
            self.process_next_data()

        except:
            print("Error in intern_process: ")

    @staticmethod
    def _get_identity_vale(row: List):
        id_val = ""
        for val in row:
            id_val += str(val) + ":"

        if id_val.endswith(":"):
            id_val = id_val[:len(id_val) - 1]

        return id_val

    def _get_schema_table(self, table_name):
        return f"{self._db_database}.{table_name}"

    def _read_tab1_value(self, columns, id_list):

        in_id = "', '".join(id_list)
        in_id = "'" + in_id + "'"
        select_sql = f"SELECT id, {', '.join(columns)} FROM {self._get_schema_table('Tab_01')} where id in ({in_id})"

        sql_cursor = self.connection.cursor()

        sql_cursor.execute(select_sql)

        sql_result = sql_cursor.fetchall()

        results = {}

        for sql_row in sql_result:
            row = [r for r in sql_row]
            row_id = row[0]
            row.pop(0)
            results[row_id] = row

        sql_cursor.close()

        return results

    @staticmethod
    def get_proper_float(in_str: str) -> float:
        if in_str is None or str(in_str).strip() == "":
            return 0
        return float(str(in_str).replace(",", "."))

    def _get_connection(self):
        try:
            db_connection = connect(
                host=self._db_host,
                user=self._db_user,
                password=self._db_password,
                port=self._db_port,
                database=self._db_database
            )

            return db_connection

        except Error as e:
            print("Error in _get_connection: " + str(e))

    def start_process(self):
        while True:
            self.connection = self._get_connection()

            self._check_run_status()
            if self._status == 1:
                self.intern_process()
            else:
                pass
            self.connection.close()

            time.sleep(self._sleep)

    def _check_run_status(self):
        try:
            values = self._read_tab1_value(["l1"], ["9POW"])
            self._status = int(values["9POW"][0])

        except:
            print("Error in _check_run_status: ")

    def get_tab_2_column_index(self, column):
        return self._tab2_columns.index(column)

    def read_tab_2_data(self):
        sql = f"SELECT {', '.join(self._tab2_columns)} FROM {self._db_database}.{self._tab2_name}"

        sql_cursor = self.connection.cursor()

        sql_cursor.execute(sql)

        sql_result = sql_cursor.fetchall()

        results = []
        for sql_row in sql_result:
            row = [r for r in sql_row]
            results.append(row)

        sql_cursor.close()

        return results

    def update_tab_3(self, id, ls, bmax):
        sql = f"delete from {self._db_database}.{self._tab3_name}"

        sql_cursor = self.connection.cursor()
        sql_cursor.execute(sql)
        self.connection.commit()

        insert_data = [id, ls, bmax]
        insert_sql = f"insert into {self._db_database}.{self._tab3_name}(id, ls, bmax) values(%s, %s, %s)"
        sql_cursor.execute(insert_sql, insert_data)

        self.connection.commit()
        sql_cursor.close()
