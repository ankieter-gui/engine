import faker
import random
import pandas as pd


def generate_data(type, amount):
    if type == '1':
        return [random.randint(0, 1) for _ in range(amount)]
    elif type == 'text':
        fake = faker.Faker(locale="pl_PL")
        return [fake.paragraph(nb_sentences=1) for _ in range(amount)]
    else:
        return [random.randint(1, int(type)) for _ in range(amount)]


questions_amount = int(input('set questions amount\n'))
answers_amount = int(input('set answers amount\n'))
survey_id = input('set survey id\n')
questions_with_answers = {}

for q in range(questions_amount):
    question = input('write question\n')
    type = input('''
set type of question
1) Numeric values (scope): e.g. 10, 20 etc.
2) Boolean values: 1 
3) String values: text
''')
    answers = generate_data(type, answers_amount)
    questions_with_answers[question] = answers

df = pd.DataFrame(questions_with_answers)
print(df)
df.to_csv('raw/' + survey_id + '.csv', index=False)
