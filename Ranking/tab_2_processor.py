from typing import List

from mysq_tyble_base import MySQLTableBase


class TableTwoConnectionProcessor(MySQLTableBase):

    def __init__(self):
        super().__init__("Tab_02", ["id", "ls", "move1", "move2", "vol1", "vol2", "LATEST"])

