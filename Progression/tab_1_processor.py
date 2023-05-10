from mysq_tyble_base import MySQLTableBase


class ConnectionOneProcessor(MySQLTableBase):

    _columns = ["id", "l1", "l2", "l3", "l4", "l5", "l6", "l7", "l8", "l9", "l10",
                "l11", "l12", "l13", "l14", "l15", "l16", "l17", "l18", "l19", "l20"]

    _data: dict = {}

    def __init__(self):
        super().__init__("Tab_01", self._columns)
        self.load_data()

    def load_data(self):
        sql_result = self.read_table_data()
        for row in sql_result:
            id = row[0]
            data_row = {self._columns[idx]: row[idx] for idx in range(0, len(self._columns))}

            self._data[id] = data_row

    def get_float_data(self, row_id: str, column: str) -> float:

        if row_id in self._data:
            if column in self._data[row_id]:
                if self._data[row_id][column] is None:
                    return 0
                return ConnectionOneProcessor.get_proper_float(self._data[row_id][column])

        return -1

    def get_int_data(self, row_id: str, column: str) -> int:
        return int(self.get_float_data(row_id, column))

    @staticmethod
    def get_proper_float(in_str: str) -> float:
        if in_str is None or str(in_str).strip() == "":
            return 0
        return float(str(in_str).replace(",", "."))

