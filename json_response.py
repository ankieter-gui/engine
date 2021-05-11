from flask_jsonpify import jsonify


class JsonResponse:
    CODE_400 = '400'
    CODE_200 = '200'

    def get_json_error_response(self, message: str):
        response = {"status_code": self.CODE_400, "error_message": message}
        return jsonify(response)

    def get_json_success_response(self, message: str, data):
        response = {"status_code": self.CODE_200, "success_message": message, "data": data}
        return jsonify(response)
