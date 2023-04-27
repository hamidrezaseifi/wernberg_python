from mysq_tyble_base import MySQLTableBase


class TableOneProcessor(MySQLTableBase):

    _columns = ["id", "l1", "l2", "l3", "l4", "l5", "l6", "l7", "l8", "l9", "l10",
                "l11", "l12", "l13", "l14", "l15", "l16", "l17", "l18", "l19", "l20"]

    _data: dict = {}

    def __init__(self):
        super().__init__()
        self.load_data()

    def load_data(self):
        print(f"Loading data from tab_1")
        read_sql = f"SELECT {', '.join(self._columns)} FROM {self._db_database}.tab_1"
        sql_result = self.read_data(read_sql)
        for row in sql_result:
            id = row[0]
            data_row = {self._columns[idx]: row[idx] for idx in range(0, len(self._columns))}

            self._data[id] = data_row

    def get_float_data(self, row_id: str, column: str) -> float:
        print(f"Getting data from tab_1 for id:{row_id} and column:{row_id}")

        if row_id in self._data:
            if column in self._data[row_id]:
                print(f"Getting data from tab_1 for id:{row_id} and column:{column} result: {self._data[row_id][column]}")
                return float(self._data[row_id][column])

        return -1

    def get_int_data(self, row_id: str, column: str) -> int:
        return int(self.get_float_data(row_id, column))

