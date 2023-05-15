import time
from datetime import datetime
from typing import List

import mysql

from config_reader import ConfigurationReader

from mysql.connector import Error, pooling


class ProgressionTableProcessor:

    _selected_table_name = ""
    _last_serie_index: int = 1
    _status: int = 1

    _serie_columns = ["id", "coin", "einsatz", "`return`", "guv"]
    _tab_4_columns = ["id", "serie", "leverage", "betrag"]

    _last_created_table_name = None

    def __init__(self):
        self.connection = None
        self.tables = []
        self.serie_tables = []
        self._table_4_name = "Tab_04"

        config = ConfigurationReader()
        self._sleep = config.get_sleep()

        self.connection_pool = self.get_connection_pool(config)
        self._db_database = config.db_database

    def load_last_row(self, table_name: str):
        if table_name not in self.serie_tables:
            return None

        sql = f"SELECT {', '.join(self._serie_columns)} FROM {self._db_database}.{table_name} order by id desc limit 1"

        sql_cursor = self.connection.cursor()
        last_row = None
        try:
            sql_cursor.execute(sql)

            sql_result = sql_cursor.fetchall()

            for sql_row in sql_result:
                last_row = [r for r in sql_row]
        except mysql.connector.Error as err:
            self.print_log("Something went wrong: {}".format(err))
        finally:
            sql_cursor.close()

        return last_row

    def is_column_not_null(self, column: str, last_row: List) -> bool:
        if last_row:
            if column in self._serie_columns:
                idx = self._serie_columns.index(column)
                return last_row[idx] is not None

        return False

    def find_first_valid_table(self) -> bool:

        self.load_tables()

        self._selected_table_name = None
        for tab_idx in range(0, len(self.serie_tables)):
            selected_table_name = self.serie_tables[tab_idx]

            last_row = self.load_last_row(selected_table_name)
            if last_row is None:
                continue
            if self.is_column_not_null("guv", last_row):
                self._selected_table_name = selected_table_name
                return True

        return False

    def add_new_row(self) -> [str, int]:
        if self._selected_table_name is not None:

            new_id = self.add_new_serire_row(self._selected_table_name)
            return new_id

        return None

    def find_new_table_name(self) -> str:

        new_table_name = f"Serie_{self._last_serie_index:06d}"

        #TABLE_LOADER.load_tables()

        while new_table_name in [t for t in self.serie_tables]:
            self._last_serie_index += 1
            if self._last_serie_index > 999999:
                self._last_serie_index = 1
            new_table_name = f"Serie_{self._last_serie_index:06d}"
        return new_table_name

    def intern_process(self):
        self.print_log("Start processing Progressions ...")

        if not self.find_first_valid_table():
            if self._last_created_table_name is None and len(self.serie_tables) > 0:
                self._last_created_table_name = self.serie_tables[len(self.serie_tables) - 1]
                self.print_log(f"Set _last_created_table_name tp '{self._last_created_table_name}'")

            if self._last_created_table_name is not None:
                last_row = self.load_last_row(self._last_created_table_name)
                if last_row is None:
                    self.print_log(f"last_row is none")
                    self.load_tables()
                    if self._last_created_table_name not in self.serie_tables:
                        self.print_log(f"'{self._last_created_table_name}' is not in {self.serie_tables}")
                        self._last_created_table_name = None
                    return

                if last_row[0] == 1 and last_row[1] is None and last_row[2] is None and last_row[3] is None and last_row[4] is None:
                    self.print_log(f"The last created table '{self._last_created_table_name}' ist not changed yet!")
                    return

            self._selected_table_name = self.find_new_table_name()

            self.create_serie_table(self._selected_table_name)

            self._last_created_table_name = self._selected_table_name

        new_id = self.add_new_row()
        values = self._read_tab1_value([f"l{new_id}"], ["5BTR", "5HEB"])

        betrag = self.get_proper_float(values["5BTR"][0])
        leverage = self.get_proper_float(values["5HEB"][0])

        self._update_tab_4(new_id, leverage, betrag, self._selected_table_name)

    def _update_tab_4(self, row_id, leverage, betrag, serie):
        sql = f"delete from {self._db_database}.{self._table_4_name}"

        insert_cursor = self.connection.cursor()
        insert_cursor.execute(sql)

        sql = f"INSERT INTO {self._db_database}.{self._table_4_name}  ({', '.join(self._tab_4_columns)}) VALUES (%s, %s, %s, %s) "

        val = (row_id, serie, leverage, betrag)

        insert_cursor.execute(sql, val)

        insert_cursor.close()

        self.connection.commit()

    def add_new_serire_row(self, table_name):

        new_id = 1
        last_row = self.load_last_row(table_name)

        if last_row is not None and len(last_row) > 0:
            last_id = int(last_row[0])
            new_id = last_id + 1

        sql = f"INSERT INTO {self._db_database}.{table_name}  ({', '.join(self._serie_columns)}) VALUES (%s, %s, %s, %s, %s)"
        val = (new_id, None, None, None, None)

        insert_cursor = self.connection.cursor()
        insert_cursor.execute(sql, val)

        insert_cursor.close()

        self.connection.commit()

        return new_id

    def create_serie_table(self, table_name):
        self.print_log(f"Creating new serie table '{table_name}' ...")

        create_sql = f"CREATE TABLE {self._db_database}.{table_name} ( id int(11) PRIMARY KEY, coin varchar(45) DEFAULT NULL, " \
                     "einsatz float DEFAULT NULL, `return` float DEFAULT NULL, guv float DEFAULT NULL)"

        create_cursor = self.connection.cursor()
        create_cursor.execute(create_sql)

        create_cursor.close()

        self.connection.commit()

    def load_tables(self):
        sql = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self._db_database}'"

        sql_cursor = self.connection.cursor()

        sql_cursor.execute(sql)

        sql_result = sql_cursor.fetchall()

        self.tables = []

        for sql_row in sql_result:
            row = [r for r in sql_row]
            self.tables.append(row[0])

        sql_cursor.close()

        self.tables.sort()

        self.serie_tables = [t for t in self.tables if t.lower().startswith("serie_")]

    def start_process(self):
        while True:
            self.connection = self.connection_pool.get_connection()
            
            self._check_run_status()
            if self._status == 1:
                self.intern_process()
            else:
                pass
            self.connection.close()
            
            time.sleep(self._sleep)

    def _check_run_status(self):

        values = self._read_tab1_value(["l1"], ["9POW"])
        self._status = int(values["9POW"][0])

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

    def _get_schema_table(self, table_name):
        return f"{self._db_database}.{table_name}"

    @staticmethod
    def get_proper_float(in_str: str) -> float:
        if in_str is None or str(in_str).strip() == "":
            return 0
        return float(str(in_str).replace(",", "."))

    def get_connection_pool(self, config):
        try:
            connection_pool = pooling.MySQLConnectionPool(pool_name="pynative_pool",
                                                          pool_size=3,
                                                          pool_reset_session=True,
                                                          host=config.db_host,
                                                          port=config.db_port,
                                                          database=config.db_database,
                                                          user=config.db_user,
                                                          password=config.db_password)

            return connection_pool
        except Error as e:
            self.print_log("Error in get_connection_pool: " + str(e))

    def print_log(self, msg):
        print(datetime.now().strftime("%Y-%m-%d %I:%M:%S") + ": " + msg)


