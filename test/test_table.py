import unittest
import sqlite3
import table
import error


class TestCase(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect('test/table.db')
        self.maxDiff = None

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
        self.assertEqual(result['share Age Rating'], expected_result['share Age Rating'])
        self.assertAlmostEqual(result['mean Price'][0], expected_result['mean Price'][0], places=7)
        self.assertAlmostEqual(result['mean Price'][1], expected_result['mean Price'][1], places=7)
        self.assertAlmostEqual(result['mean Price'][2], expected_result['mean Price'][2], places=7)

    def test_good_two(self):
        query = {
            "get": [["Price", "Name"]],
            "as": ["mean", "share"],
            "by": ["Age Rating", "*"],
            "if": [["Age Rating", "notin", "4"]]
        }
        result = table.create(query, self.conn)
        self.assertTrue(all(value == 1 or value == 2 for value in result['share Name'][0].values()))

    def test_good_three(self):
        query = {
            "get": [["Price"]],
            "as": ["mean", "share"],
            "by": ["Age Rating", "*"],
            "if": [["Age Rating", "notin", "4"]]
        }
        expected_result = {
            'index': [9, 12, 17, '*'],
            'mean Price': [0.6346535326086957, 0.8920405101275319, 0.30010380622837374, 0.7142954104718812],
            'share Price': [
                {0.0: 1193, 0.99: 83, 1.99: 42, 2.99: 53, 3.99: 29, 4.99: 42, 5.99: 5, 6.99: 8, 7.99: 3, 9.99: 10,
                 14.99: 1, 19.99: 3},
                {0.0: 1042, 0.99: 46, 1.99: 39, 2.99: 75, 3.99: 23, 4.99: 61, 5.99: 7, 6.99: 6, 7.99: 2, 9.99: 26,
                 11.99: 1, 12.99: 3, 14.99: 1, 19.99: 1},
                {0.0: 262, 0.99: 8, 1.99: 3, 2.99: 5, 3.99: 1, 4.99: 8, 6.99: 2},
                {0.0: 2497, 0.99: 137, 1.99: 84, 2.99: 133, 3.99: 53, 4.99: 111, 5.99: 12, 6.99: 16, 7.99: 5, 9.99: 36,
                 11.99: 1, 12.99: 3, 14.99: 2, 19.99: 4}]}
        result = table.create(query, self.conn)
        self.assertEqual(result['share Price'], expected_result['share Price'])
        self.assertAlmostEqual(result['mean Price'][0], expected_result['mean Price'][0], places=7)
        self.assertAlmostEqual(result['mean Price'][1], expected_result['mean Price'][1], places=7)
        self.assertAlmostEqual(result['mean Price'][2], expected_result['mean Price'][2], places=7)
        self.assertAlmostEqual(result['mean Price'][3], expected_result['mean Price'][3], places=7)

    def test_good_four(self):
        query = {
            "get": [["Price"]],
            "as": ["share"],
            "if": [["Age Rating", "notin", 4], [0, "<=", 1]]
        }
        expected_result = {
            'index': ['*'],
            'share Price': [{0.0: 2497, 0.99: 137}]
        }
        result = table.create(query, self.conn)
        self.assertEqual(result, expected_result)

    def test_good_five(self):
        query = {
            "get": [["Price"]],
            "as": ["count", "rows"],
            "if": [[0, "=", 1], [0, "!=", 1]]
        }
        expected_result = {
            'count Price': [0],
            'index': ['*'],
            'rows Price': [7561]
        }
        result = table.create(query, self.conn)
        self.assertEqual(result, expected_result)

    def test_good_six(self):
        query = {
            "get": [["Price"]],
            "as": ["share"],
            "if": [["Age Rating", "notin", 4], [0, "<=", "1"]]
        }
        expected_result = {
            'index': ['*'],
            'share Price': [{0.0: 2497, 0.99: 137}]
        }
        result = table.create(query, self.conn)
        self.assertEqual(result, expected_result)

    def test_good_seven(self):
        query = {
            "get": [["Price"]],
            "as": ["max"],
            "if": [["Age Rating", "notin", 4], [0, "<=", "1"]]
        }
        expected_result = {
            'index': ['*'],
            'max Price': [0.99]
        }
        result = table.create(query, self.conn)
        self.assertEqual(result, expected_result)

    def test_good_eight(self):
        query = {
            "get": [["Price"]],
            "as": ["max"],
            "if": [["Age Rating", "notin", 4], [0, "<=", "1"], [0, "notin", 0.99]]
        }
        expected_result = {
            'index': ['*'],
            'max Price': [0.0]
        }
        result = table.create(query, self.conn)
        self.assertEqual(result, expected_result)

    def test_good_nine(self):
        query = {
            "get": [["Age Rating"]],
            "as": ["share"],
            "if": [[0, "!=", 4]]
        }
        expected_result = {
            'index': ['*'],
            'share Age Rating': [{9: 1472, 12: 1333, 17: 289}]
        }
        result = table.create(query, self.conn)
        self.assertEqual(result, expected_result)

    def test_good_ten(self):
        query = {
            "get": [["Age Rating"]],
            "as": ["share"],
            "if": [["Age Rating", "!=", 4]],
            "except": [["Price", "!=", 0.0], ["Age Rating", ">", 12]]
        }
        expected_result = {
            'index': ['*'],
            'share Age Rating': [{9: 1472, 12: 1333, 17: 262}]
        }
        result = table.create(query, self.conn)
        self.assertEqual(result, expected_result)

    def test_good_eleven(self):
        query = {
            "get": [["Double Age Rating"]],
            "as": ["share"],
            "if": [["Age Rating", "!=", 4]],
            "except": [["Price", "!=", 0.0], ["Age Rating", ">", 12]],
            "join": [
                {
                    'name': 'Double Age Rating',
                    'of': ['Age Rating', 'Age Rating'],
                }
            ],
        }
        expected_result = {
            'index': ['*'],
            'share Double Age Rating': [{9: 2*1472, 12: 2*1333, 17: 2*262}]
        }
        result = table.create(query, self.conn)
        self.assertEqual(result, expected_result)

    def test_bad_zero(self):
        query = {
            "get": [["Price"]],
            "as": ["share"],
            "if": [["Age Rating", "notin", 4], [1, "<=", 1]]
        }
        with self.assertRaises(error.API):
            _ = table.create(query, self.conn)

    def test_bad_one(self):
        query = {
            "get": [["Name"]],
            "as": ["share"],
            "if": [[0, ">=", "D"], [1, "<=", "D"]]
        }
        with self.assertRaises(error.API):
            _ = table.create(query, self.conn)

    def test_bad_two(self):
        query = {
            "get": [["Price", "Name"]],
            "as": ["mean", "share", "max"],
            "by": ["Age Rating", "*"],
            "if": [["Age Rating", "notin", "4"]]
        }
        with self.assertRaises(error.API):
            _ = table.create(query, self.conn)

    def test_bad_three(self):
        query = {
            "get": [["Price", "Price"],
                    ["Average User Rating", "Average User Rating"]],
            "as": ["mean", "var"],
            "by": ["Age Rating", "*"],
            "if": [["9"]]
        }
        with self.assertRaises(error.API):
            _ = table.create(query, self.conn)

    def test_bad_four(self):
        query = {
            "get": [["Price", "Name", "Price"]],
            "as": ["mean", "share"],
            "by": ["Age Rating", "*"],
            "if": [["Age Rating", "notin", "4"]]
        }
        with self.assertRaises(error.API):
            _ = table.create(query, self.conn)

    def test_bad_five(self):
        query = {
            "get": [["Price", "Age Rating"]],
            "as": ["mean", "share"],
            "by": ["Age Rating", "*"],
            "if": [["Name", ">", "4"]]
        }
        with self.assertRaises(error.API):
            _ = table.create(query, self.conn)

    def test_bad_six(self):
        query = {
            "get": [["Price", "Age Rating"]],
            "as": ["mean", "share"],
            "by": ["Age Rating", "*"],
            "if": [["Age Rating", ">", "4", "9"]]
        }
        with self.assertRaises(error.API):
            _ = table.create(query, self.conn)

    def test_bad_seven(self):
        query = {
            "get": [["Price", "Name"]],
            "as": ["count", "mean"],
            "by": ["Age Rating", '*'],
        }
        with self.assertRaises(error.API):
            _ = table.create(query, self.conn)

    def test_bad_eight(self):
        query = {
            "get": [["Price", 4]],
            "as": ["mean", "share"],
            "by": ["Age Rating", "*"],
            "if": [["Age Rating", "in", "4", "9"]]
        }
        with self.assertRaises(error.API):
            _ = table.create(query, self.conn)

    def test_bad_nine(self):
        query = {
            "as": [],
            "by": [],
            "filter": [],
            "get": []
        }
        with self.assertRaises(error.API):
            _ = table.create(query, self.conn)


if __name__ == '__main__':
    unittest.main()
