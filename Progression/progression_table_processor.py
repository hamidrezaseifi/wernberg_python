from typing import List

from process_base import ProcessBase

#from table_loader import TABLE_LOADER



class ProgressionTableProcessor(ProcessBase):

    _selected_table_name = ""
    _last_serie_index: int = 27
    _status: int = 1

    _serie_columns = ["id", "coin", "einsatz", "`return`", "guv"]
    _tab_4_columns = ["id", "serie", "leverage", "betrag"]


    def __init__(self):
        super().__init__()
        self.tables = []
        self.serie_tables = []
        self._table_4_name = "Tab_04"

    def load_last_row(self, table_name: str, db_connection):
        sql = f"SELECT {', '.join(self._serie_columns)} FROM {self._db_database}.{table_name} order by id desc limit 1"

        sql_cursor = db_connection.cursor()

        sql_cursor.execute(sql)

        sql_result = sql_cursor.fetchall()

        last_row = None
        for sql_row in sql_result:
            last_row = [r for r in sql_row]

        return last_row

    def is_column_not_null(self, column: str, last_row: List) -> bool:
        if last_row:
            if column in self._serie_columns:
                idx = self._serie_columns.index(column)
                return last_row[idx] is not None

        return False

    def find_first_valid_table(self) -> bool:
        #TABLE_LOADER.load_tables()

        #tables = TABLE_LOADER.get_serie_tables()

        db_connection = self._get_connection()

        self.load_tables(db_connection)

        self._selected_table_name = None
        for tab_idx in range(0, len(self.serie_tables)):
            selected_table_name = self.serie_tables[tab_idx]

            last_row = self.load_last_row(selected_table_name, db_connection)

            if self.is_column_not_null("guv", last_row):
                self._selected_table_name = selected_table_name

                db_connection.close()
                return True

        db_connection.close()
        return False

    def add_new_row(self) -> [str, int]:
        if self._selected_table_name is not None:
            #serie_table = SerireConnectionProcessor(self._selected_table_name)
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
        db_connection = self._get_connection()

        sql = f"delete from {self._db_database}.{self._table_4_name}"

        insert_cursor = db_connection.cursor()
        insert_cursor.execute(sql)

        sql = f"INSERT INTO {self._db_database}.{self._table_4_name}  ({', '.join(self._tab_4_columns)}) VALUES (%s, %s, %s, %s) "

        val = (row_id, serie, leverage, betrag)

        insert_cursor = db_connection.cursor()
        insert_cursor.execute(sql, val)

        db_connection.commit()
        db_connection.close()

    def add_new_serire_row(self, table_name):
        db_connection = self._get_connection()

        new_id = 1
        last_row = self.load_last_row(table_name, db_connection)
        if last_row is not None and len(last_row) > 0:
            last_id = int(last_row[0])
            new_id = last_id + 1

        sql = f"INSERT INTO {self._db_database}.{table_name}  ({', '.join(self._serie_columns)}) VALUES (%s, %s, %s, %s, %s)"
        val = (new_id, None, None, None, None)

        insert_cursor = db_connection.cursor()
        insert_cursor.execute(sql, val)

        db_connection.commit()
        db_connection.close()

        return new_id

    def create_serie_table(self, table_name):
        print(f"Creating new serie table '{table_name}' ...")

        create_sql = f"CREATE TABLE {self._db_database}.{table_name} ( id int(11) PRIMARY KEY, coin varchar(45) DEFAULT NULL, " \
                     "einsatz float DEFAULT NULL, `return` float DEFAULT NULL, guv float DEFAULT NULL)"

        db_connection = self._get_connection()
        create_cursor = db_connection.cursor()
        create_cursor.execute(create_sql)

        db_connection.commit()
        db_connection.close()

    def load_tables(self, db_connection):
        sql = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self._db_database}'"

        sql_cursor = db_connection.cursor()

        sql_cursor.execute(sql)

        sql_result = sql_cursor.fetchall()

        self.tables = []

        for sql_row in sql_result:
            row = [r for r in sql_row]
            self.tables.append(row[0])

        self.tables.sort()

        self.serie_tables = [t for t in self.tables if t.lower().startswith("serie_")]


