import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from database import *
from globals import app
from datetime import datetime
import unittest


class TestCase(unittest.TestCase):

    def setUp(self):
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        db.create_all()
        self.survey = Survey(
            id=1,
            Name='Ankieta testowa',
            StartedOn=datetime.now(),
            EndsOn=datetime.now(),
            IsActive=1,
            BackgroundImg=random.choice(os.listdir('bkg')))
        db.session.add(self.survey)
        db.session.commit()

    def tearDown(self):
        db.drop_all()

    def test_get_survey(self):
        expected = self.survey
        result = get_survey(self.survey.id)
        self.assertEqual(result, expected)

    def test_rename_survey(self):
        new_name = 'Ankieta testowa 2'
        rename_survey(self.survey, new_name)
        self.assertEqual(self.survey.Name, new_name)

    def test_delete_survey(self):
        delete_survey(self.survey)
        with self.assertRaises(error.API):
            get_survey(self.survey.id)


if __name__ == '__main__':
    unittest.main()
