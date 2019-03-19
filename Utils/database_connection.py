import sqlite3


class DatabaseConnection:
    def __init__(self, db_path):
        self.connection = None
        self.db_path = db_path

    def __enter__(self):
        # self.connection = sqlite3.connect('C:\\sqlite\\chinook.db')
        self.connection = sqlite3.connect(self.db_path)
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.commit()
        self.connection.close()



"""
    cursor = connection.cursor()
    cursor.execute('SELECT * FROM employees')
    for row in cursor:
        print("row = " + str(row))
"""