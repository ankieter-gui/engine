class Generic(Exception):
    def __init__(self, message: str):
        self.message = message

    def add_details(self, message: str):
        """Add details to the error message.

        Keyword arguments:
        message -- details to be concatenated

        Return value:
        returns self
        """

        self.message = f'{message}: {self.message}'
        return self


class API(Generic):
    def as_dict(self) -> dict:
        """Return message as a dict.

        Return value:
        returns dict object
        """

        return {'error': self.message}
