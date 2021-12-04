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
        self.user2 = User(
            CasLogin='user',
            Pesel='9999999999',
            Role='u',
            FetchData=False
        )
        self.user_group = UserGroup(
            UserId=1,
            Group='student'
        )
        self.user_group2 = UserGroup(
            UserId=1,
            Group='wmi'
        )
        self.user_group3 = UserGroup(
            UserId=2,
            Group='student'
        )
        db.session.add(self.survey)
        db.session.add(self.report)
        db.session.add(self.user)
        db.session.add(self.user2)
        db.session.add(self.user_group)
        db.session.add(self.user_group2)
        db.session.add(self.user_group3)
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

    def test_create_report(self):
        new_report = create_report(self.user, self.survey, 'Nowy raport', 1)
        expected_report = get_report(new_report.id)
        self.assertEqual(new_report, expected_report)

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

    def test_create_user(self):
        new_user = create_user('test', '9999999997', 'u')
        expected_user = get_user(new_user.id)
        self.assertEqual(new_user, expected_user)
        delete_user(new_user)

    def test_get_all_users(self):
        result_json = get_all_users()
        expected_json = {"users": [{"casLogin": "admin", "id": 1}, {"casLogin": "user", "id": 2}]}
        self.assertEqual(result_json, expected_json)

    def test_delete_user(self):
        delete_user(self.user)
        with self.assertRaises(error.API):
            get_user(self.user.id)

    def test_get_groups(self):
        result = get_groups()
        expected = ['student', 'wmi']
        self.assertListEqual(result, expected)

    def test_get_user_groups(self):
        result = get_user_groups(self.user)
        expected = ['student', 'wmi']
        self.assertListEqual(result, expected)

    def test_set_user_groups(self):
        set_user_group(self.user2, 'dyrektor')
        self.assertListEqual(['dyrektor', 'student'], get_user_groups(self.user2))


if __name__ == '__main__':
    unittest.main()
