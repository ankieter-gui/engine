class Generic(Exception):
    def __init__(self, message):
        self.message = message

    def add_details(self, message):
        self.message = f'{message}: {self.message}'
        return self

class API(Generic):
    def as_dict(self):
        return {'error': self.message}
