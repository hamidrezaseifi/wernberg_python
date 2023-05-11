
import mysql.connector

from config_reader import CONFIG_READER


class MySQLConnectionBase:

    _db_database: str = None
    _db_host: str = None
    _db_port: str = None
    _db_user: str = None
    _db_password: str = None

    def __init__(self):

        self._db_host = CONFIG_READER.db_host
        self._db_port = CONFIG_READER.db_port
        self._db_user = CONFIG_READER.db_user
        self._db_password = CONFIG_READER.db_password
        self._db_database = CONFIG_READER.db_database

    def _get_connection(self):
        db_connection = mysql.connector.connect(
            host=self._db_host,
            user=self._db_user,
            password=self._db_password,
            port=self._db_port,
            database=self._db_database
        )

        return db_connection

    def get_schema(self) -> str:
        return self._db_database



