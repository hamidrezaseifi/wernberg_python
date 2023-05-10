
from mysq_tyble_base import MySQLTableBase
from serie_table_processor import SerireConnectionProcessor
from process_base import ProcessBase
from tab_1_processor import ConnectionOneProcessor
from tab_4_processor import TableFourConnectionProcessor
import logging

from table_loader import TABLE_LOADER


class ProgressionTableProcessor(ProcessBase):

    _selected_table_index = -1
    _selected_table_name = ""
    _selected_table: SerireConnectionProcessor = None
    _last_serie_index: int = 27
    _tab_1: ConnectionOneProcessor = None
    _tab_4: TableFourConnectionProcessor
    _status: int = 1


    def __init__(self, sleep_seconds: int):
        super().__init__(sleep_seconds)
        logging.warning('Starting Progression modules.')
        self._tab_1 = ConnectionOneProcessor()
        self._tab_4 = TableFourConnectionProcessor()
        self.check_run_status()

    def find_first_valid_table(self) -> bool:
        TABLE_LOADER.load_tables()

        tables = TABLE_LOADER.get_serie_tables()

        for tab_idx in range(0, len(tables)):
            self._selected_table_name = tables[tab_idx]
            self._selected_table = SerireConnectionProcessor(self._selected_table_name)
            self._selected_table.load_last_row()
            if self._selected_table.is_column_not_null("guv"):
                self._selected_table_index = tab_idx
                return True

        logging.info(f"There is no valid table in exiting tables!")
        return False

    def add_new_row(self) -> [str, int]:
        if self._selected_table:
            new_id = self._selected_table.add_new_row()

            return self._selected_table.get_table_name(), new_id

        return None, None

    def find_new_table_name(self) -> str:

        new_table_name = f"Serie_{self._last_serie_index:06d}"

        TABLE_LOADER.load_tables()

        while new_table_name.lower() in [t.lower() for t in TABLE_LOADER.get_serie_tables()]:
            self._last_serie_index += 1
            if self._last_serie_index > 999999:
                self._last_serie_index = 1
            new_table_name = f"Serie_{self._last_serie_index:06d}"
        return new_table_name

    def intern_process(self):
        logging.info("Start processing Progressions ...")

        if not self.find_first_valid_table():
            new_table_name = self.find_new_table_name()

            self._selected_table = SerireConnectionProcessor(new_table_name)
            self._selected_table.create_table()

        table_name, new_id = self.add_new_row()
        betrag = self._tab_1.get_float_data("5BTR", f"l{new_id}")
        leverage = self._tab_1.get_float_data("5HEB", f"l{new_id}")

        self._tab_4 .update(new_id, leverage, betrag, table_name)


