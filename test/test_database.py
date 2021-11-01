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
            BackgroundImg=random.choice(os.listdir('bkg'))
        )
        self.report = Report(
            id=1,
            Name='Raport testowy',
            SurveyId=self.survey.id,
            BackgroundImg=self.survey.BackgroundImg,
            AuthorId=1
        )
        self.user = User(
            CasLogin='admin',
            Pesel='9999999999',
            Role='s',
            FetchData=False
        )
        db.session.add(self.survey)
        db.session.add(self.report)
        db.session.add(self.user)
        db.session.commit()

    def tearDown(self):
        db.drop_all()

    def test_get_survey(self):
        expected = self.survey
        result = get_survey(self.survey.id)
        self.assertEqual(result, expected)

    def test_rename_survey(self):
        expected = 'Ankieta testowa 2'
        rename_survey(self.survey, expected)
        result = self.survey.Name
        self.assertEqual(result, expected)

    def test_delete_survey(self):
        delete_survey(self.survey)
        with self.assertRaises(error.API):
            get_survey(self.survey.id)

    def test_create_survey(self):
        new_survey = create_survey(self.user, 'Nowa ankieta')
        expected_survey = get_survey(new_survey.id)
        self.assertEqual(new_survey, expected_survey)

    def test_get_report(self):
        expected = self.report
        result = get_report(self.report.id)
        self.assertEqual(result, expected)

    def test_rename_report(self):
        expected = 'Raport testowy 2'
        rename_report(self.report, expected)
        result = self.report.Name
        self.assertEqual(result, expected)

    def test_delete_report(self):
        delete_report(self.report)
        with self.assertRaises(error.API):
            get_report(self.report.id)


if __name__ == '__main__':
    unittest.main()
