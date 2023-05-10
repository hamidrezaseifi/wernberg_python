
import csv
import os

from mysq_connection_base import MySQLConnectionBase
from mysq_tyble_base import MySQLTableBase
from tab_2_processor import TableTwoConnectionProcessor

def get_float(val: str)-> float:
    if str(val).lower() == "null":
        return None
    return float(val.replace(",", "."))


files = []
root_path = "E:\\Hamid\\Projects\\python\\wernberg\\test_data"
for file_name in os.listdir(root_path):
    if file_name.endswith(".txt"):
        file_path = os.path.join(root_path, file_name)
        files.append(file_path)

table = TableTwoConnectionProcessor()

files = ["E:\\Hamid\\Projects\\python\\wernberg\\test_data\\Mappe1.CSV"]
insert_sql = "INSERT INTO test.tab_2(id, ls, move1, move2, vol1, vol2, latest) VALUES (%s, %s, %s, %s, %s, %s, %s);"
columns = ["id", "ls", "move1", "move2", "vol1", "vol2", "LATEST"]


for file_path in files:
    print()
    print("--------------------------------------------------------------------------------------------")
    print()
    print(file_path)
    #db_connection = table.get_connection()
    #insert_cursor = db_connection.cursor()

    with open(file_path, newline='') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=';', quotechar='|')
        for row in csv_reader:
            if row[0] == "ID":
                continue


            data_row = [get_float(r) if i > 1 else r for i,r in enumerate(row)]
            data_row_dic = {}
            for i in range(0, len(columns)):
                data_row_dic[columns[i]] = data_row[i]
            table.insert_data(data_row_dic)
            #insert_cursor.execute(insert_sql, data_row)
            print(' , '.join(row))

    #db_connection.commit()
    #db_connection.close()
