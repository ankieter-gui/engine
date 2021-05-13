import sqlite3



def get_column_types(conn):
    column_types = {}
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(data)")
    data = cur.fetchall()

    for row in data:
        column_types[row[1]] = row[2]
    return column_types
