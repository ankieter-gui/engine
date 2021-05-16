class Error(Exception):
    def __init__(self, message):
        self.message = message

    def add_details(self, message):
        self.message = f'{message}: {self.message}'
        return self

class APIError(Error):
    def to_json():
        return {'error': self.message}
