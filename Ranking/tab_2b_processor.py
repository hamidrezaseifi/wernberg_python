from typing import Dict
from typing import List

from mysq_tyble_base import MySQLTableBase


class TableTwoBTableProcessor(MySQLTableBase):

    _columns = ["id", "ls", "move1", "move2", "vol1", "vol2", "last", "ergebnis"]
    _table_name: str = "tab_2_b"

    def __init__(self):
        super().__init__()

        myresult = self.read_data(
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{self.get_schema()}' and table_name = '{self._table_name}'")
        if len(myresult) == 0:
            self._create_table()

    def load_data(self):
        print(f"Loading data from tab_2")
        db_connection = self._get_connection()


        db_connection.close()

    def insert_data(self, data: List[Dict]):
        print(f"Loading data into {self.get_schema()}.{self._table_name}")

        insert_sql = f"INSERT INTO {self.get_schema()}.{self._table_name}({','.join(self._columns)}) " \
                     f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
        db_connection = self._get_connection()

        for row in data:
            sql_cursor = db_connection.cursor()
            insert_data = [row[c] for c in self._columns]
            sql_cursor.execute(insert_sql, insert_data)

        db_connection.commit()
        db_connection.close()

    def _create_table(self):
        _create_sql = f"CREATE TABLE {self.get_schema()}.{self._table_name} (id varchar(45) NOT NULL PRIMARY KEY,ls int(4) NOT NULL," \
                      "move1 float8 DEFAULT NULL,move2 float8 DEFAULT NULL,vol1 float8 DEFAULT NULL," \
                      "vol2 float8 DEFAULT NULL,last float8 DEFAULT NULL,ergebnis float8 DEFAULT NULL)"
        db_connection = self._get_connection()
        sql_cursor = db_connection.cursor()
        sql_cursor.execute(_create_sql)
        db_connection.close()

