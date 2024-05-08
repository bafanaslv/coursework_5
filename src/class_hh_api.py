from src.class_abstract import HHapiABC
import requests


class HeadHunterAPI(HHapiABC):
    """Класс для получения данных с HeadHAnter."""
    def __init__(self):
        self.status_code = 0

    def get_status_code(self) -> int:
        """Возврат статуса запроса к ресурсу url_get."""
        return self.status_code

    def get_vacancies(self, url_get, params) -> str:
        """Получение вакансий с ресурса url_get и возврат их в json-формате используя словарь с параметрами params."""
        response = requests.get(url_get, params)
        self.status_code = response.status_code
        return response.json()
