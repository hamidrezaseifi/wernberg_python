import time
from abc import ABC

from config_reader import CONFIG_READER
#from tab_1_processor import ConnectionOneProcessor
import mysql.connector

class ProcessBase(ABC):
    _status: int = 1
    _sleep: float = 1


    def __init__(self):
        self._sleep = CONFIG_READER.get_sleep()

        self._db_host = CONFIG_READER.db_host
        self._db_port = CONFIG_READER.db_port
        self._db_user = CONFIG_READER.db_user
        self._db_password = CONFIG_READER.db_password
        self._db_database = CONFIG_READER.db_database

        self._check_run_status()

    def _get_connection(self):
        db_connection = mysql.connector.connect(
            host=self._db_host,
            user=self._db_user,
            password=self._db_password,
            port=self._db_port,
            database=self._db_database
        )

        return db_connection
    def intern_process(self):
        pass

    def start_process(self):
        while True:
            self._check_run_status()
            if self._status == 1:
                self.intern_process()
            else:
                pass
            time.sleep(self._sleep)

    def _check_run_status(self):
        #_tab_1 = ConnectionOneProcessor()

        values = self._read_tab1_value(["l1"], ["9POW"])
        self._status = int(values["9POW"][0])

    #def _read_tab1_int_value(self, columns, id_list):
    #    values = self._read_tab1_value(columns, id_list)
    #    return values[0][0]
    #def _read_tab1_float_value(self, columns, id_list):
    #    pass

    def _read_tab1_value(self, columns, id_list):

        in_id = "', '".join(id_list)
        in_id = "'" + in_id + "'"
        select_sql = f"SELECT id, {', '.join(columns)} FROM {self._get_schema_table('Tab_01')} where id in ({in_id})"

        db_connection = self._get_connection()

        sql_cursor = db_connection.cursor()

        sql_cursor.execute(select_sql)

        sql_result = sql_cursor.fetchall()

        results = {}

        for sql_row in sql_result:
            row = [r for r in sql_row]
            row_id = row[0]
            row.pop(0)
            results[row_id] = row

        sql_cursor.close()
        db_connection.close()

        return results

    def _get_schema_table(self, table_name):
        return f"{self._db_database}.{table_name}"

    @staticmethod
    def get_proper_float(in_str: str) -> float:
        if in_str is None or str(in_str).strip() == "":
            return 0
        return float(str(in_str).replace(",", "."))
