#!/usr/bin/python

from datetime import datetime, timedelta
from itertools import product
from functools import reduce
from typing import List
from database import *
import random
import os


def ask(question, choices):
    while True:
        print(f'{question} ({", ".join([str(c) for c in choices])}) [{choices[0] if choices else ""}]', end=' ')
        choice = input().strip()
        if len(choice) == 0:
            if choices:
                choice = choices[0]
            else:
                continue
        if choices:
            try:
                if type(choices[0]) is int:
                    choice = int(choice)
                elif type(choices[0]) is float:
                    chocie = float(choice)
            except:
                continue
        if choice in choices or len(choices) <= 1:
            break
    return choice


def setup(admin, pesel):
    db.drop_all()
    db.create_all()

    for directory in ['data', 'raw', 'report', 'bkg']:
        if not os.path.exists(directory):
            os.makedirs(directory)

    backgrounds = os.listdir('bkg')

    db.session.add(User(CasLogin=admin,
                        Pesel=pesel,
                        Role='s',
                        FetchData=False))

    db.session.add(User(CasLogin=GUEST_NAME,
                        Pesel='99999999998',
                        Role='g',
                        FetchData=True))
    db.session.commit()

if __name__ == "__main__":
    if not os.path.exists('config.py') or 't' == ask('Konfiguracja już istnieje, czy zamierzasz utworzyć ją na nowo?', ['n', 't']):
        CAS_URL = ask('Pełny adres serwera CAS', ['https://cas.amu.edu.pl/cas/'])
        CAS_VERSION = ask('Wersja systemu CAS', [2, 1, 3])

        APP_URL = ask('Adres, na którym hostowana jest aplikacja', ['https://ankieter.projektstudencki.pl'])
        if APP_URL.endswith('/'):
            APP_URL = APP_URL[:-1]

        APP_PORT = ask('Port, na którym hostowana jest apliakcja', [443])
        SSL_CERT = ask('Ścieżka do certyfikatu *.pem', ['/etc/letsencrypt/live/ankieter.projektstudencki.pl/fullchain.pem'])
        SSL_PVKY = ask('Ścieżka do klucza prywantego *.pem', ['/etc/letsencrypt/live/ankieter.projektstudencki.pl/privkey.pem'])

        DEBUG = 't' == ask('Czy aplikacja ma być uruchamiana w trybie testowym?', ['t', 'n'])

        with open('config.py', 'w+') as cfg:
            print(f"CAS_URL='{CAS_URL}'", file=cfg)
            print(f"CAS_VERSION={CAS_VERSION}", file=cfg)
            print(f"APP_URL='{APP_URL}'", file=cfg)
            print(f"APP_PORT={APP_PORT}", file=cfg)
            print(f"SSL_CONTEXT=('{SSL_CERT}', '{SSL_PVKY}')", file=cfg)
            print(f"DEBUG={DEBUG}", file=cfg)
        print("Zapisano konfigurację. Można ją później zmienić w pliku config.py")

    if not os.path.exists('master.db') or 't' == ask('Baza danych użytkowników już istnieje, czy zamierzasz utworzyć ją na nowo?', ['n', 't']):
        admin = ask('Nazwa użytkownika CAS, który będzie administratorem', ['admin'])
        pesel = ask('PESEL administratora (umożliwi logowanie na dwa sposoby)', [99999999999])
        setup(admin=admin, pesel=pesel)
