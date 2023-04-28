
from mysq_tyble_base import MySQLTableBase


class TableThreeConnectionProcessor(MySQLTableBase):

    def __init__(self):
        super().__init__("tab_3", ["id", "ls", "bmax"])


