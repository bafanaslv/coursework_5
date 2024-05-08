# Данная функция предназначена постраничного получения вакансий hh_vacancies с HeadHanter.
# Результатом работы функции являетяеся список объектов вакансий и список словарей вакансий.

from src.class_hh_api import HeadHunterAPI
from src.class_vacancy import Vacancy


def create_vacancies_list(params, page_quantity, url) -> list:
    """Функция предназначена для работы с API ресурса HeadHater для получения вакансий."""
    hh_api = HeadHunterAPI()  # создание экземпляра поискового класса HeadHanter
    # Получение вакансий с hh.ru в формате JSON.
    vacancies_dict_list = []     # список словарей вакансий
    vacancies_objects_list = []  # список объектов вакансий
    page = 0
    while page < page_quantity:
        params["page"] = page
        hh_vacancies = hh_api.get_vacancies(url, params)
        if hh_api.get_status_code() == 200:  # если запрос прошел удачно, то идем дальше.
            # Из полученных из json-файла списка словарей hh_vacancies получаем
            # список оъектов вакансий и список словарей вакансий с помощью метода hh_api.get_vacancies.
            vacancies_objects_list = Vacancy.create_objects_vacancy(hh_vacancies, vacancies_dict_list)
            page += 1
        else:
            print(f'Ответ: {hh_api.get_status_code()} - не удалось получить доступ к ресурсу HeadHater.')
            break
    return vacancies_objects_list
