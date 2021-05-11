from json_response import JsonResponse


class RequestSurvey:
    SURVEY_ID = 'survey_id'

    def __init__(self, json_data) -> None:
        self.json_data = json_data

    def execute_request(self):
        if self.SURVEY_ID not in self.json_data:
            return JsonResponse(f'Parameter {self.SURVEY_ID} not found').get_json_error_response()

        return JsonResponse('All done').get_json_success_response()
