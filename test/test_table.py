import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], '..'))

import unittest
import sqlite3
import table
from error import *


class TestCase(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect('test/table.db')

    def tearDown(self):
        self.conn.close()

    def test_good_one(self):
        query = {
            "get": [["Price", "Age Rating"]],
            "as": ["mean", "share"],
            "by": ["Age Rating", "*Total"],
            "if": [["Age Rating", "in", "4", "9"]]
        }
        expected_result = {
            'index': [4, 9, 'Total'],
            'mean Price': [0.4722655025744348, 0.6346535326086957, 0.5125138912274794],
            'share Age Rating': [{4: 4467}, {9: 1472}, {4: 4467, 9: 1472}]
        }
        result = table.create(query, self.conn)
        self.assertEqual(result, expected_result)


# good = []
# bad = []
#
# good.append({
#     "get": [["Price", "Age Rating"]],
#     "as": ["mean", "share"],
#     "by": ["Age Rating", "*Total"],
#     "if": [["Age Rating", "in", "4", "9"]]
# })
#
# good.append({
#     "get": [["Price", "Name"]],
#     "as": ["mean", "share"],
#     "by": ["Age Rating", "*"],
#     "if": [["Age Rating", "notin", "4"]]
# })
#
# good.append({
#     "get": [["Price"]],
#     "as": ["mean", "share"],
#     "by": ["Age Rating", "*"],
#     "if": [["Age Rating", "notin", "4"]]
# })
#
# good.append({
#     "get": [["Price"]],
#     "as": ["share"],
#     "if": [["Age Rating", "notin", 4], [0, "<=", 1]]
# })
#
# good.append({
#     "get": [["Price"]],
#     "as": ["count", "rows"],
#     "if": [[0, "=", 1], [0, "!=", 1]]
# })
#
# good.append({
#     "get": [["Price"]],
#     "as": ["share"],
#     "if": [["Age Rating", "notin", 4], [0, "<=", "1"]]
# })
#
# good.append({
#     "get": [["Price"]],
#     "as": ["max"],
#     "if": [["Age Rating", "notin", 4], [0, "<=", "1"]]
# })
#
# good.append({
#     "get": [["Price"]],
#     "as": ["max"],
#     "if": [["Age Rating", "notin", 4], [0, "<=", "1"], [0, "notin", 0.99]]
# })
#
# good.append({
#     "get": [["Age Rating"]],
#     "as": ["share"],
#     "if": [[0, "!=", 4]]
# })
#
# bad.append({
#     "get": [["Price"]],
#     "as": ["share"],
#     "if": [["Age Rating", "notin", 4], [1, "<=", 1]]
# })
#
# bad.append({
#     "get": [["Name"]],
#     "as": ["share"],
#     "if": [[0, ">=", "D"], [1, "<=", "D"]]
# })
#
# bad.append({
#     "get": [["Price", "Name"]],
#     "as": ["mean", "share", "max"],
#     "by": ["Age Rating", "*"],
#     "if": [["Age Rating", "notin", "4"]]
# })
#
# bad.append({
#     "get": [["Price",               "Price"],
#             ["Average User Rating", "Average User Rating"]],
#     "as": ["mean", "var"],
#     "by": ["Age Rating", "*"],
#     "if": [["9"]]
# })
#
# bad.append({
#     "get": [["Price", "Name", "Price"]],
#     "as": ["mean", "share"],
#     "by": ["Age Rating", "*"],
#     "if": [["Age Rating", "notin", "4"]]
# })
#
# bad.append({
#     "get": [["Price", "Age Rating"]],
#     "as": ["mean", "share"],
#     "by": ["Age Rating", "*"],
#     "if": [["Name", ">", "4"]]
# })
#
# bad.append({
#     "get": [["Price", "Age Rating"]],
#     "as": ["mean", "share"],
#     "by": ["Age Rating", "*"],
#     "if": [["Age Rating", ">", "4", "9"]]
# })
#
# bad.append({
#     "get": [["Price", "Name"]],
#     "as": ["count", "mean"],
#     "by": ["Age Rating", '*'],
# })
#
# bad.append({
#     "get": [["Price", 4]],
#     "as": ["mean", "share"],
#     "by": ["Age Rating", "*"],
#     "if": [["Age Rating", "in", "4", "9"]]
# })
#
# bad.append({
#     "as": [],
#     "by": [],
#     "filter": [],
#     "get": []
# })

# for query in good:
#     try:
#         r = create(query, conn)
#         print(r)
#     except error.API as err:
#         print(query, err.message)
#
# for query in bad:
#     try:
#         r = create(query, conn)
#     except error.API as err:
#         continue
#     raise error.Generic('test fail')
#

if __name__ == '__main__':
    unittest.main()