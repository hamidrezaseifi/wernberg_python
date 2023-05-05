import logging

from mysq_tyble_base import MySQLTableBase


class SerireConnectionProcessor(MySQLTableBase):

    _columns = ["id", "coin", "einsatz", "`return`", "guv"]
    _last_row = None

    def __init__(self, table_name: str):
        super().__init__(table_name, self._columns)

    def load_last_row(self):
        logging.info(f"loading last row from {self._table_name}")
        sql = f"SELECT {', '.join(self._columns)} FROM {self._db_database}.{self._table_name} order by id desc limit 1"
        sql_result = self.read_sql_data(sql)
        for row in sql_result:
            self._last_row = {self._columns[idx]: row[idx] for idx in range(0, len(self._columns))}

    def is_column_not_null(self, column: str) -> bool:
        if self._last_row:
            if column in self._last_row:
                return self._last_row[column] is not None

        return False

    def add_new_row(self) -> int:
        logging.debug(f"Writing new row to {self._table_name}")
        new_id = 1
        if self._last_row:
            last_id = self._last_row["id"]
            new_id = last_id + 1

        db_connection = self._get_connection()

        sql = f"INSERT INTO {self._db_database}.{self._table_name}  ({', '.join(self._columns)}) VALUES (%s, %s, %s, %s, %s)"
        val = (new_id, None, None, None, None)

        insert_cursor = db_connection.cursor()
        insert_cursor.execute(sql, val)

        db_connection.commit()
        db_connection.close()

        return new_id

    def get_table_name(self):
        return self._table_name

    def create_table(self):
        logging.info(f"Creating new serie table '{self._table_name}' ...")

        create_sql = f"CREATE TABLE {self._db_database}.{self._table_name} ( id int(11) PRIMARY KEY, coin varchar(45) DEFAULT NULL, " \
                     "einsatz float DEFAULT NULL, `return` float DEFAULT NULL, guv float DEFAULT NULL)"
        self.execute_sql(create_sql)


