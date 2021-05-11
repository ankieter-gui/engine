from flask_jsonpify import jsonify


class JsonResponse:
    CODE_400 = '400'
    CODE_200 = '200'

    def __init__(self, error_message: str) -> None:
        self.error_message = error_message

    def get_json_error_response(self):
        response = {"status_code": self.CODE_400, 'error_message': self.error_message}
        return jsonify(response)

    def get_json_success_response(self):
        response = {"status_code": self.CODE_200, 'success_message': self.error_message}
        return jsonify(response)
