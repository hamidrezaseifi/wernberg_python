from typing import List

from mysq_tyble_base import MySQLTableBase


class TableLoader(MySQLTableBase):

    _tables: List[str] = []
    _serie_tables: List[str] = []

    def __init__(self):
        super().__init__("tables", [])

        self.load_tables()

    def load_tables(self):
        myresult = self.read_sql_data(
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{self.get_schema()}'")

        self._tables = [r[0] for r in myresult]

        self._tables.sort()

        self._serie_tables = [t for t in self._tables if t.lower().startswith("serie_")]

    def get_tables(self) -> List[str] :
        return self._tables

    def get_serie_tables(self) -> List[str] :
        return self._serie_tables

    def get_serie_tables_lower(self) -> List[str] :
        return [t.lower() for t in self._serie_tables]

    def get_tables_lower(self):
        return [t.lower() for t in self._tables]


TABLE_LOADER: TableLoader = TableLoader()
