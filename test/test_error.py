import unittest
import error


class TestCase(unittest.TestCase):

    def setUp(self):
        self.err = error.API("API error")

    def test_add_details_to_error(self):
        self.err.add_details("empty survey data")
        self.assertEqual(self.err.message, "empty survey data: API error")

    def test_error_as_dict(self):
        error_dict = self.err.as_dict()
        self.assertEqual(error_dict, {'error': 'API error'})
