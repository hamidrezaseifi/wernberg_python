import logging

from mysq_tyble_base import MySQLTableBase
from table_loader import TABLE_LOADER


class TableFourConnectionProcessor(MySQLTableBase):

    _columns = ["id", "serie", "leverage", "betrag"]

    def __init__(self):
        super().__init__("Tab_04", self._columns)

        if self._table_name.lower() not in TABLE_LOADER.get_tables_lower():
            create_sql = "CREATE TABLE `Tab_04` (`id` int(11) PRIMARY KEY,`serie` varchar(45) COLLATE utf8_unicode_ci NOT NULL, `leverage` int(11) DEFAULT NULL,`betrag` float DEFAULT NULL)"
            self.execute_sql(create_sql)

    def update(self, row_id: int, leverage: float, betrag: float, serie: str):
        logging.debug(f"Writing new row to Tab_04")
        db_connection = self._get_connection()

        sql = f"delete from {self._db_database}.{self._table_name}"

        insert_cursor = db_connection.cursor()
        insert_cursor.execute(sql)

        sql = f"INSERT INTO {self._db_database}.{self._table_name}  ({', '.join(self._columns)}) VALUES (%s, %s, %s, %s) " \
              f"ON DUPLICATE KEY UPDATE serie=%s , leverage=%s , betrag=%s"
        val = (row_id, serie, leverage, betrag, serie, leverage, betrag)

        insert_cursor = db_connection.cursor()
        insert_cursor.execute(sql, val)


        db_connection.commit()
        db_connection.close()

