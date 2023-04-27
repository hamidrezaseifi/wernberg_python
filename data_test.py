
import csv
import os

from mysq_connection_base import MySQLConnectionBase

files = []
root_path = "E:\\Hamid\\Projects\\python\\wernberg\\test_data"
for file_name in os.listdir(root_path):
    if file_name.endswith(".txt"):
        file_path = os.path.join(root_path, file_name)
        files.append(file_path)

table = MySQLConnectionBase()

insert_sql = "INSERT INTO test.tab_2(id, ls, move1, move2, vol1, vol2, last) VALUES (%s, %s, %s, %s, %s, %s, %s);"

for file_path in files:
    print()
    print("--------------------------------------------------------------------------------------------")
    print()
    print(file_path)
    db_connection = table.get_connection()
    insert_cursor = db_connection.cursor()

    with open(file_path, newline='') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=';', quotechar='|')
        for row in csv_reader:
            if row[0] == "ID":
                continue

            data_row = [float(r.replace(",", ".")) if i > 0 else r for i,r in enumerate(row)]
            insert_cursor.execute(insert_sql, data_row)
            print(' , '.join(row))

    db_connection.commit()
    db_connection.close()
