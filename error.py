class Generic(Exception):
    def __init__(self, message: str):
        self.message = message
        self.data = {}

    def add_details(self, message: str):
        """Add details to the error message.

        :param str message: details to be concatenated
        :rtype: self
        """

        self.message = f'{message}: {self.message}'
        return self


class API(Generic):
    def as_dict(self) -> dict:
        """Return message as a dict.

        :return: Message as dict
        :rtype: dict
        """

        err = {**self.data, 'error': self.message}
        return err
