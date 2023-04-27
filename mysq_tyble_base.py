from typing import List

from mysq_connection_base import MySQLConnectionBase


class MySQLTableBase(MySQLConnectionBase):

    _table_name: str = None
    _select_columns: List[str] = []
    _insert_columns: List[str] = []
    _update_columns: List[str] = []

    def __init__(self,
                 table_name: str,
                 columns: List[str],
                 insert_columns: List[str] = None,
                 update_columns: List[str] = None):
        super().__init__()
        self._table_name = table_name
        self._select_columns = columns

        self._insert_columns = columns
        if insert_columns is not None:
            self._insert_columns = insert_columns

        self._update_columns = columns
        if update_columns is not None:
            self._update_columns = update_columns


    def get_connection(self):
        return self._get_connection()

    def read_table_data(self):
        select_sql = f"SELECT {', '.join(self._select_columns)} FROM {self._db_database}.{self._table_name}"

        return self.read_sql_data(select_sql)

    def read_sql_data(self, sql: str) -> List:
        db_connection = self._get_connection()

        sql_cursor = db_connection.cursor()

        sql_cursor.execute(sql)

        sql_result = sql_cursor.fetchall()

        results = []

        for r in sql_result:
            results.append(r)

        sql_cursor.close()
        db_connection.close()

        return results
