import time
from typing import List

from config_reader import ConfigurationReader

from mysql.connector import connect, Error


class ProgressionTableProcessor:

    _selected_table_name = ""
    _last_serie_index: int = 27
    _status: int = 1

    _serie_columns = ["id", "coin", "einsatz", "`return`", "guv"]
    _tab_4_columns = ["id", "serie", "leverage", "betrag"]

    def __init__(self):
        self.connection = None
        self.tables = []
        self.serie_tables = []
        self._table_4_name = "Tab_04"

        config = ConfigurationReader()
        self._sleep = config.get_sleep()

        self._db_host = config.db_host
        self._db_port = config.db_port
        self._db_user = config.db_user
        self._db_password = config.db_password
        self._db_database = config.db_database

    def load_last_row(self, table_name: str):
        sql = f"SELECT {', '.join(self._serie_columns)} FROM {self._db_database}.{table_name} order by id desc limit 1"

        sql_cursor = self.connection.cursor()

        sql_cursor.execute(sql)

        sql_result = sql_cursor.fetchall()

        last_row = None
        for sql_row in sql_result:
            last_row = [r for r in sql_row]

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

        while new_table_name.lower() in [t.lower() for t in self.serie_tables]:
            self._last_serie_index += 1
            if self._last_serie_index > 999999:
                self._last_serie_index = 1
            new_table_name = f"Serie_{self._last_serie_index:06d}"
        return new_table_name

    def intern_process(self):
        print("Start processing Progressions ...")

        if not self.find_first_valid_table():
            self._selected_table_name = self.find_new_table_name()

            self.create_serie_table(self._selected_table_name)

        new_id = self.add_new_row()

        #_tab_1 = ConnectionOneProcessor()

        #_tab_1.load_data()

        values = self._read_tab1_value([f"l{new_id}"], ["5BTR", "5HEB"])

        betrag = self.get_proper_float(values["5BTR"][0]) #_tab_1.get_float_data("5BTR", f"l{new_id}")
        leverage = self.get_proper_float(values["5HEB"][0]) #_tab_1.get_float_data("5HEB", f"l{new_id}")

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
        print(f"Creating new serie table '{table_name}' ...")

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


