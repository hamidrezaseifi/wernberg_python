import time
from datetime import datetime
from typing import List

import mysql

from config_reader import ConfigurationReader

from mysql.connector import Error, pooling


class ProgressionTableProcessor:

    #_selected_table_name = ""
    #_last_serie_index: int = 1
    status: int = 1

    serie_columns = ["id", "coin", "einsatz", "`return`", "guv"]
    tab_4_columns = ["id", "serie", "leverage", "betrag"]

    #_last_created_table_name = None

    def __init__(self):
        self.connection = None
        self.tables = []
        self.serie_tables = []
        self.table_4_name = "Tab_04"

        config = ConfigurationReader()
        self.sleep = config.get_sleep()

        self.connection_pool = self.get_connection_pool(config)
        self.db_database = config.db_database

    def load_last_row(self, table_name: str):
        if table_name not in self.serie_tables:
            return None

        sql = f"SELECT {', '.join(self.serie_columns)} FROM {self.db_database}.{table_name} order by id desc limit 1"

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

    @staticmethod
    def is_full_data_row(last_row: List) -> bool:
        if last_row:
            null_values = [r for r in last_row if r is None]
            if len(null_values) > 0:
                return False
            return True

        return False

    @staticmethod
    def is_writeable_data_row(last_row: List) -> bool:
        if last_row:
            if last_row[0] is not None and last_row[1] is None and last_row[2] is None and last_row[3] is None and last_row[4] is None:
                return True

        return False

    def find_first_writeable_table(self) -> [str, bool]:

        self.load_tables()

        full_tables_name = None
        writeable_table = None

        for tab_idx in range(0, len(self.serie_tables)):
            selected_table_name = self.serie_tables[tab_idx]

            last_row = self.load_last_row(selected_table_name)
            if last_row is None:
                continue
            if ProgressionTableProcessor.is_full_data_row(last_row):
                if full_tables_name is None:
                    full_tables_name = selected_table_name

            if ProgressionTableProcessor.is_writeable_data_row(last_row):
                if writeable_table is None:
                    writeable_table = {"table": selected_table_name, "last_id": int(last_row[0])}

        return full_tables_name, writeable_table is not None

    def find_new_table_name(self) -> str:

        last_id = 0
        if len(self.serie_tables) > 0:
            last_serie = self.serie_tables[len(self.serie_tables) - 1]
            last_serie_id = last_serie[6:]
            last_id = int(last_serie_id)
        new_id = last_id + 1

        new_table_name = self.get_serie_name(new_id)

        while new_table_name.lower() in [t.lower() for t in self.serie_tables]:
            new_id += 1
            if new_id > 999999:
                new_id = 1
            new_table_name = self.get_serie_name(new_id)
        return new_table_name

    def get_serie_name(self, new_id):
        new_table_name = f"Serie_{new_id:06d}"
        return new_table_name

    def intern_process(self):
        self.print_log("Start processing Progressions ...")

        full_tables_name, writeable = self.find_first_writeable_table()
        if full_tables_name is None and not writeable:
            self.create_new_table()
        else:
            if full_tables_name is not None:
                self.add_new_row(full_tables_name)
            else:
                self.print_log("There is write-able serie tables. No action ...")
            #    if status == "writeable":
            #        self.update_tab_4(last_id, selected_table)

    def update_tab_4(self, row_id, serie):
        values = self.read_tab1_value([f"l{row_id}"], ["5BTR", "5HEB"])

        betrag = self.get_proper_float(values["5BTR"][0])
        leverage = self.get_proper_float(values["5HEB"][0])

        self.print_log(f"Update Tab_04 row_id:{row_id}, leverage:{leverage}, betrag:{betrag}, serie:{serie}")
        sql = f"delete from {self.db_database}.{self.table_4_name}"

        insert_cursor = self.connection.cursor()
        insert_cursor.execute(sql)

        sql = f"INSERT INTO {self.db_database}.{self.table_4_name}  ({', '.join(self.tab_4_columns)}) VALUES (%s, %s, %s, %s) "

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

        sql = f"INSERT INTO {self.db_database}.{table_name}  ({', '.join(self.serie_columns)}) VALUES (%s, %s, %s, %s, %s)"
        val = (new_id, None, None, None, None)

        insert_cursor = self.connection.cursor()
        insert_cursor.execute(sql, val)

        insert_cursor.close()

        self.connection.commit()

        return new_id

    def create_serie_table(self, table_name):
        self.print_log(f"Creating new serie table '{table_name}' ...")

        create_sql = f"CREATE TABLE {self.db_database}.{table_name} ( id int(11) PRIMARY KEY, coin varchar(45) DEFAULT NULL, " \
                     "einsatz float DEFAULT NULL, `return` float DEFAULT NULL, guv float DEFAULT NULL)"

        create_cursor = self.connection.cursor()
        create_cursor.execute(create_sql)

        create_cursor.close()

        self.connection.commit()

    def load_tables(self):
        sql = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.db_database}'"

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
            
            self.check_run_status()
            if self.status == 1:
                self.intern_process()
            else:
                pass
            self.connection.close()
            
            time.sleep(self.sleep)

    def check_run_status(self):

        values = self.read_tab1_value(["l1"], ["9POW"])
        self.status = int(values["9POW"][0])

    def read_tab1_value(self, columns, id_list):

        in_id = "', '".join(id_list)
        in_id = "'" + in_id + "'"
        select_sql = f"SELECT id, {', '.join(columns)} FROM {self.get_schema_table('Tab_01')} where id in ({in_id})"

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

    def get_schema_table(self, table_name):
        return f"{self.db_database}.{table_name}"

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
            self.load_tables()

    def print_log(self, msg):
        print(datetime.now().strftime("%Y-%m-%d %I:%M:%S") + ": " + msg)

    def create_new_table(self):
        new_table_name = self.find_new_table_name()

        self.create_serie_table(new_table_name)

        new_id = self.add_new_serire_row(new_table_name)

        self.update_tab_4(new_id, new_table_name)

    def add_new_row(self, selected_table):
        new_id = self.add_new_serire_row(selected_table)

        self.update_tab_4(new_id, selected_table)


