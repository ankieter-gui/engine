import unittest
from datetime import datetime
import random
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from database import *
from config import app


class TestCase(unittest.TestCase):


    def setUp(self):
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        db.create_all()
        bkgs = os.listdir('bkg')
        self.survey = Survey(
            id=1,
            Name='Ankieta testowa',
            StartedOn=datetime.now(),
            EndsOn=datetime.now(),
            IsActive=1,
            BackgroundImg=random.choice(bkgs))
        db.session.add(self.survey)
        db.session.commit()


    def tearDown(self):
        db.drop_all()


    def test_get_survey(self):
        expected = self.survey
        result = get_survey(1)
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
