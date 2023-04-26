

from mysq_tyble_base import MySQLTableBase


class TableTwoTableProcessor(MySQLTableBase):

    _columns = ["id", "ls", "move1", "move2", "vol1", "vol2", "last"]

    def __init__(self):
        super().__init__()

    def load_data(self):
        print(f"Loading data from tab_2")
        db_connection = self._get_connection()


        db_connection.close()

