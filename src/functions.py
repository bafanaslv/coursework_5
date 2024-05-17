# Данная функция предназначена постраничного получения вакансий hh_vacancies с HeadHanter.
# Результатом работы функции являетяеся список вакансий и список работодателей.

from src.class_db_manager import DBManager
from src.class_hh_api import HeadHunterAPI
from config import ROOT_DIR
import configparser
import json

CONNECTION_FILE = ROOT_DIR+'/database.ini'  # конфигурационный файл для подключения к БД


def connector(connection_file) -> dict:
    """Функция предназанчена для считывания файла с параметрами подключения к БД возврата данных в виде словаря."""
    config = configparser.ConfigParser()
    config.read(connection_file)
    database_config = dict(config.items('database'))
    return database_config


def read_employers_list(params, page_quantity, url) -> list:
    """Функция предназначена для работы с API ресурсом HeadHater для получения вакансий."""
    hh_api = HeadHunterAPI()  # создание экземпляра поискового класса HeadHanter
    # Получение вакансий с hh.ru в формате JSON.
    employers_list = []  # список работодателей
    page = 0  # номер страницы HeadHanter

    while page < page_quantity:
        params["page"] = page
        hh_vacancies = hh_api.get_vacancies(url, params)
        if hh_api.get_status_code() == 200:  # если запрос прошел удачно, то идем дальше.
            # Из полученного в json-формате списка hh_vacancies получаем
            # список вакансий и список работодателей помощью функци create_vacancy_lists.
            employers_list = create_employer_lists(hh_vacancies, employers_list)
            page += 1
        else:
            print(f'Ответ: {hh_api.get_status_code()} - не удалось получить доступ к ресурсу HeadHater.')
            break
    return employers_list


def create_employer_lists(hh_vacancies, employers_list) -> list:
    """Функция для создания списка вакансий и списка работодателей из списка словарей hh_vacancies,
     полученных с HeadHanter. В спиской работодателей попадают попадают только уникальные,то есть отстуствуют
     повторы. Параллельно при заполнении списка работодателей подсчитывется количество вакансий каждого работодателя.
     Это поле нужно для сортировки, чтобы затем выбрать 10 работодателей с наибольшим количеством вакансий."""
    if isinstance(hh_vacancies, dict):
        for vacancy in hh_vacancies["items"]:
            salary_min, salary_max, currency = salary_valid(vacancy['salary'])
            if salary_min > salary_max:
                salary = salary_min
            else:
                salary = salary_max
            if salary_min == 0 and salary_max == 0:
                continue
            # пропускаем если не указана зарплата
            if "id" not in vacancy["employer"]:
                continue
            # пропускаем если отсутствует идентификатор работодателя
            if len(employers_list) == 0:
                employers_list.append([vacancy['employer']['id'],
                                       vacancy['employer']['name'], vacancy['alternate_url'], salary])
            else:
                # если идентификатор работодателя уже есть в списке, то пропускаем.
                list_append = False
                for emp_list in employers_list:
                    if emp_list[0] == vacancy['employer']['id']:
                        if salary > emp_list[3]:
                            emp_list[3] = salary
                        list_append = True
                if list_append is False:
                    employers_list.append([vacancy['employer']['id'],
                                           vacancy['employer']['name'], vacancy['alternate_url'], salary])

        return employers_list
    else:
        print('Ошибочный формат файла.')


def read_vacancies_list(params, page_quantity, selected_employers, url) -> list:
    """Функция предназначена для работы с API ресурсом HeadHater для получения вакансий."""
    hh_api = HeadHunterAPI()  # создание экземпляра поискового класса HeadHanter
    # Получение вакансий с hh.ru в формате JSON.
    vacancies_list = []  # список вакансий
    page = 0  # номер страницы HeadHanter

    while page < page_quantity:
        params["page"] = page
        hh_vacancies = hh_api.get_vacancies(url, params)
        if hh_api.get_status_code() == 200:  # если запрос прошел удачно, то идем дальше.
            # Из полученного в json-формате списка hh_vacancies получаем
            # список вакансий и список работодателей помощью функци create_vacancy_lists.
            vacancies_list = create_vacancy_lists(hh_vacancies, selected_employers, vacancies_list)
            page += 1
        else:
            print(f'Ответ: {hh_api.get_status_code()} - не удалось получить доступ к ресурсу HeadHater.')
            break
    return vacancies_list


def create_vacancy_lists(hh_vacancies, employers_list, vacancies_list) -> list:
    """Функция для создания списка вакансий и списка работодателей из списка словарей hh_vacancies,
     полученных с HeadHanter. В спиской работодателей попадают попадают только уникальные,то есть отстуствуют
     повторы. Параллельно при заполнении списка работодателей подсчитывется количество вакансий каждого работодателя.
     Это поле нужно для сортировки, чтобы затем выбрать 10 работодателей с наибольшим количеством вакансий."""
    if isinstance(hh_vacancies, dict):
        for vacancy in hh_vacancies["items"]:
            # пропускаем если отсутствует идентификатор работодателя
            if "id" not in vacancy["employer"]:
                continue
            # валидация зарплаты salary_valid и должностных обязанностей responsibility_valid
            for sel in employers_list:
                if vacancy['employer']['id'] == sel[0]:
                    salary_min, salary_max, currency = salary_valid(vacancy['salary'])
                    responsibility = responsibility_valid(vacancy['snippet'])
                    vacancies_list.append([vacancy["id"],
                                           vacancy["name"],
                                           vacancy['area']['name'],
                                           vacancy['snippet']['requirement'],
                                           responsibility,
                                           salary_min,
                                           salary_max,
                                           currency,
                                           vacancy['employer']['id']])
        return vacancies_list
    else:
        print('Ошибочный формат файла.')


def responsibility_valid(responsibility_item) -> str:
    """Валидация должностных обязанностей."""
    if not responsibility_item:
        return 'не указан'
    else:
        if not responsibility_item["responsibility"]:
            return 'не указан'
        else:
            return responsibility_item["responsibility"]


def salary_valid(salary_item) -> (int, int, str):
    """Валидация зарплаты."""
    if salary_item is None:
        salary_min = 0
        salary_max = 0
        currency = ''
    else:
        if not salary_item['from']:
            salary_min = 0
        else:
            salary_min = salary_item['from']

        if not salary_item['to']:
            salary_max = 0
        else:
            salary_max = salary_item['to']

        if not salary_item['currency']:
            currency = ''
        else:
            currency = salary_item['currency']
    return salary_min, salary_max, currency


def save_json_file(selected_emp_10, json_file):
    """Запись в json-файл."""
    employers_dict_list = []
    for sel in selected_emp_10:
        employer_dict = {"id": sel[0],
                         "name": sel[1],
                         "employer_url": sel[2]}
        employers_dict_list.append(employer_dict)
    with open(json_file, 'w', encoding="UTF-8") as file:
        employers_json = json.dumps(employers_dict_list, ensure_ascii=False, indent=4)
        file.write(employers_json)


def load_json_file(json_file) -> list:
    """Загрузка json-файла."""
    selected_emp_10 = []
    with open(json_file, 'r', encoding='utf-8') as file:
        employers_dict = json.load(file)
    for dict in employers_dict:
        list = [dict["id"], dict["name"], dict["employer_url"]]
        selected_emp_10.append(list)
    return selected_emp_10


def database_manager(selected_emp_10, selected_vac):
    database_config_dict = connector(CONNECTION_FILE)
    db_manager = DBManager(database_config_dict)  # создание класса для работы с БД PostgreSQL.
    # create_database - метод для создания экземпляра БД hh_vacancies.
    # connect_database - метод для подключения к БД hh_vacancies.
    # create_tables - метод для создания таблиц БД (employers - работодатели, vacancies - вакансии).
    if not db_manager.create_database():
        # выведется ошибка создания пустой БД
        pass
    elif not db_manager.connect_database():
        # выведется ошибка подключения к вновь созданной БД
        pass
    elif not db_manager.create_tables(selected_emp_10, selected_vac):
        # выведется ошибка создание таблиц БД
        pass
    else:
        while True:
            print("1.Получить список компаний и количество вакансий у каждой компании\n"
                  "2.Получить список всех вакансий с указанием названия компании, "
                  "названия вакансии и зарплаты и ссылки на ваканси\n"
                  "3.Получить среднюю зарплату по вакансиям\n"
                  "4.Получить список всех вакансий, у которых зарплата выше средней по всем вакансиям\n"
                  "5.Получить список всех вакансий, в названии которых содержатся переданные в метод слова\n"
                  "0.Завершение работы программы\n")
            answer = input("Ввеедите номер запроса:")  # ввод номера пункта меню
            if answer == '1':
                db_manager.get_companies_and_vacancies_count()
                print("")
            elif answer == '2':
                db_manager.get_all_vacancies()
                print("")
            elif answer == '3':
                db_manager.get_avg_salary()
                print("")
            elif answer == '4':
                db_manager.get_vacancies_with_higher_salary()
                print("")
            elif answer == '5':
                keywords = input("Введите ключевые слова в названии вакансии через запятую:\n")
                if len(keywords) > 0:
                    db_manager.get_vacancies_with_keyword(keywords)
                    print("")
                else:
                    print("Ключевые слова не введены - программа завершает работу !")
            else:
                break
    db_manager.close_database()
