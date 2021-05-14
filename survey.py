import sqlite3

def get_types(conn):
    types = {}
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(data)")
    data = cur.fetchall()

    for row in data:
        types[row[1]] = row[2]
    return types
