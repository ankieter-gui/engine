#!/usr/bin/python3
import sqlite3
import sys
from distutils.core import setup

def create_database():
    con = sqlite3.connect('master.db')
    cur = con.cursor()
    with open('setup.sql', 'r') as f:
        queries = f.read().split(';')
    for query in queries:
        cur.execute(query)
    return

if __name__ == "__main__":
    create_database()
