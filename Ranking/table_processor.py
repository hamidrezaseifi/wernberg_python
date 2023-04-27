import time

from mysq_connection_base import MySQLConnectionBase
from tab_1_processor import ConnectionOneProcessor


class MainTableProcessor:
    _tables = []
    _selected_table_index = -1
    _selected_table_name = ""
    _selected_table: MySQLTableReader = None
    _last_serie_index: int = 27
    _tab_1: ConnectionOneProcessor = None
    _status: int = 1

    def __init__(self):
        self._tab_1 = ConnectionOneProcessor()
        self.check_run_status()

    def find_first_valid_table(self) -> bool:
        table = MySQLConnectionBase()
        myresult = table.read_data(
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{table.get_schema()}' and table_name like 'serie_%'")

        self._tables = [r[0] for r in myresult]

        self._tables.sort()

        for tab_idx in range(0, len(self._tables)):
            self._selected_table_name = self._tables[tab_idx]
            self._selected_table = MySQLTableReader(self._selected_table_name)
            self._selected_table.load_last_row()
            if self._selected_table.is_column_not_null("guv"):
                self._selected_table_index = tab_idx
                print(f"First valid table is {self._selected_table_name} at index {self._selected_table_index}")

                return True

        print(f"There is no valid table in exiting tables!")
        return False

    def add_new_row(self) -> [str, int]:
        if self._selected_table:
            new_id = self._selected_table.add_new_row()

            return self._selected_table.get_table_name(), new_id

        return None, None

    def find_new_table_name(self) -> str:
        new_table_name = f"serie_{self._last_serie_index:06d}"
        while new_table_name in self._tables:
            self._last_serie_index += 1
            if self._last_serie_index > 999999:
                self._last_serie_index = 1
            new_table_name = f"serie_{self._last_serie_index:06d}"
        return new_table_name

    def process_next_table(self):
        print("Start processing tables ...")

        if not self.find_first_valid_table():
            new_table_name = self.find_new_table_name()

            self._selected_table = MySQLTableReader(new_table_name)
            self._selected_table.create_table()

        table_name, new_id = self.add_new_row()
        betrag = self._tab_1.get_float_data("5BTR", f"l{new_id}")
        leverage = self._tab_1.get_float_data("5HEB", f"l{new_id}")

        tab_4 = TableFourReader()
        tab_4.insert(new_id, leverage, betrag, table_name)

    def process_tables(self):
        while True:
            if self._status == 1:
                self.process_next_table()
            else:
                print("Status is paused!")
            time.sleep(1)

    def check_run_status(self):
        self._tab_1.load_data()
        self._status = self._tab_1.get_int_data("9POW", "l1")


