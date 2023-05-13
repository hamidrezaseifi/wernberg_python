from mysql.connector import Error, pooling, connect

from config_reader import ConfigurationReader

config = ConfigurationReader()

db_database = config.db_database


def get_connection_pool():
    try:
        pool = pooling.MySQLConnectionPool(pool_name="pynative_pool",
                                           pool_size=3,
                                           pool_reset_session=True,
                                           host=config.db_host,
                                           port=config.db_port,
                                           database=config.db_database,
                                           user=config.db_user,
                                           password=config.db_password)

        return pool
    except Error as e:
        print("Error in get_connection_pool: " + str(e))


def get_connection():
    try:
        connection = connect(host=config.db_host,
                             port=config.db_port,
                             database=config.db_database,
                             user=config.db_user,
                             password=config.db_password)

        return connection
    except Error as e:
        print("Error in get_connection_pool: " + str(e))


def get_serire_list():
    sql = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{db_database}'"

    connection = get_connection()

    sql_cursor = connection.cursor()

    sql_cursor.execute(sql)

    sql_result = sql_cursor.fetchall()

    tables = []

    for sql_row in sql_result:
        row = [r for r in sql_row]
        tables.append(row[0])

    sql_cursor.close()
    connection.close()

    tables.sort()

    serie_tables = [t for t in tables if t.lower().startswith("serie_")]

    return serie_tables


def exec_drop_sql(drop_sql):
    connection = get_connection()

    sql_cursor = connection.cursor()

    sql_cursor.execute(drop_sql)

    #connection.commit()

    connection.close()
    print(drop_sql)


if __name__ == '__main__':
    config = ConfigurationReader()

    # connection_pool = get_connection_pool()

    serie_tables = get_serire_list()

    print(serie_tables)

    drop_chunk = 20

    while len(serie_tables) > 0:
        drop_sql = ""
        item_count = 0
        for tab in serie_tables:
            drop_sql += f"DROP TABLE IF EXISTS {tab}; \n"
            item_count += 1
            serie_tables.remove(tab)
            if item_count >= drop_chunk:
                break

        exec_drop_sql(drop_sql)
