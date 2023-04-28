import logging

from mysq_tyble_base import MySQLTableBase


class TableFourConnectionProcessor(MySQLTableBase):

    _columns = ["id", "serie", "leverage", "betrag"]

    def __init__(self):
        super().__init__("tab_4", self._columns)

    def insert(self, row_id: int, leverage: float, betrag: float, serie: str):
        logging.debug(f"Writing new row to tab_4")
        db_connection = self._get_connection()

        sql = f"INSERT INTO {self._db_database}.tab_4  ({', '.join(self._columns)}) VALUES (%s, %s, %s, %s) " \
              f"ON DUPLICATE KEY UPDATE serie=%s , leverage=%s , betrag=%s"
        val = (row_id, serie, leverage, betrag, serie, leverage, betrag)

        insert_cursor = db_connection.cursor()
        insert_cursor.execute(sql, val)

        db_connection.commit()
        db_connection.close()

