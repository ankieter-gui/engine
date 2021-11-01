import test

import sqlite3
import database
import table
import error

conn = sqlite3.connect('test/table.db')

good = []
bad = []

good.append({
    "get": [["Price", "Age Rating"]],
    "as": ["mean", "share"],
    "by": ["Age Rating", "*Total"],
    "if": [["Age Rating", "in", "4", "9"]]
})

good.append({
    "get": [["Price", "Name"]],
    "as": ["mean", "share"],
    "by": ["Age Rating", "*"],
    "if": [["Age Rating", "notin", "4"]]
})

good.append({
    "get": [["Price"]],
    "as": ["mean", "share"],
    "by": ["Age Rating", "*"],
    "if": [["Age Rating", "notin", "4"]]
})

bad.append({
    "get": [["Price",               "Price"],
            ["Average User Rating", "Average User Rating"]],
    "as": ["mean", "var"],
    "by": ["Age Rating", "*"],
    "if": [["9"]]
})

bad.append({
    "get": [["Price", "Name", "Price"]],
    "as": ["mean", "share"],
    "by": ["Age Rating", "*"],
    "if": [["Age Rating", "notin", "4"]]
})

bad.append({
    "get": [["Price", "Age Rating"]],
    "as": ["mean", "share"],
    "by": ["Age Rating", "*"],
    "if": [["Name", ">", "4"]]
})

bad.append({
    "get": [["Price", "Age Rating"]],
    "as": ["mean", "share"],
    "by": ["Age Rating", "*"],
    "if": [["Age Rating", ">", "4", "9"]]
})

bad.append({
    "get": [["Price", "Name"]],
    "as": ["count", "mean"],
    "by": ["Age Rating", '*'],
})

bad.append({
    "get": [["Price", 4]],
    "as": ["mean", "share"],
    "by": ["Age Rating", "*"],
    "if": [["Age Rating", "in", "4", "9"]]
})

bad.append({
    "as": [],
    "by": [],
    "filter": [],
    "get": []
})

for query in good:
    try:
        r = table.create(query, conn)
    except error.API as err:
        print(query, err.message)

for query in bad:
    try:
        r = table.create(query, conn)
    except error.API as err:
        continue
    raise error.Generic('test fail')

conn.close()
