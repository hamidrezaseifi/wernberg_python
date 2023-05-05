
from mysq_tyble_base import MySQLTableBase
from table_loader import TABLE_LOADER


class TableThreeConnectionProcessor(MySQLTableBase):

    def __init__(self):
        super().__init__("Tab_03", ["id", "ls", "bmax"])

        if self._table_name.lower() not in TABLE_LOADER.get_tables_lower():
            create_sql = "CREATE TABLE Tab_03 (`id` varchar(45) COLLATE utf8_unicode_ci PRIMARY KEY, `ls` int(4) NOT NULL, `bmax` double DEFAULT NULL)"
            self.execute_sql(create_sql)

