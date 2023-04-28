from typing import List

from mysq_tyble_base import MySQLTableBase


class TableTwoConnectionProcessor(MySQLTableBase):

    def __init__(self):
        super().__init__("tab_2", ["id", "ls", "move1", "move2", "vol1", "vol2", "last"])

