import configparser
import os
from os.path import dirname
from typing import List

import mysql.connector


class MySQLTableBase:

    _db_database: str = None
    _db_host: str = None
    _db_port: str = None
    _db_user: str = None
    _db_password: str = None

    def __init__(self):
        db_conf_file = os.path.join(dirname(__file__), "db.ini")
        if not os.path.exists(db_conf_file):
            raise ValueError(f"Database config file '{db_conf_file}' not found!")
        config = configparser.ConfigParser()
        config.read(db_conf_file)
        self._db_host = config['DEFAULT']['host']
        self._db_port = config['DEFAULT']['port']
        self._db_user = config['DEFAULT']['user']
        self._db_password = config['DEFAULT']['password']
        self._db_database = config['DEFAULT']['database']

    def _get_connection(self):
        db_connection = mysql.connector.connect(
            host=self._db_host,
            user=self._db_user,
            password=self._db_password,
            port=self._db_port,
            database=self._db_database
        )

        return db_connection

    def create_statement(self, sql: str) -> List:
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

    def get_schema(self) -> str:
        return self._db_database



