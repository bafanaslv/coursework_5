# Данная функция предназначена постраничного получения вакансий hh_vacancies с HeadHanter.
# Результатом работы функции являетяеся список вакансий и список работодателей.

from src.class_hh_api import HeadHunterAPI
import configparser


def connector(connection_file) -> dict:
    """Функция предназанчена для считывания файла с параметрами подключения к БД возврата данных в виде словаря."""
    config = configparser.ConfigParser()
    config.read(connection_file)
    database_config = dict(config.items('database'))
    return database_config


def read_vacancies_list(params, page_quantity, url) -> (list, list):
    """Функция предназначена для работы с API ресурсом HeadHater для получения вакансий."""
    hh_api = HeadHunterAPI()  # создание экземпляра поискового класса HeadHanter
    # Получение вакансий с hh.ru в формате JSON.
    employers_list = []  # список работодателей
    vacancies_list = []  # список вакансий
    page = 0  # номер страницы HeadHanter

    while page < page_quantity:
        params["page"] = page
        hh_vacancies = hh_api.get_vacancies(url, params)
        if hh_api.get_status_code() == 200:  # если запрос прошел удачно, то идем дальше.
            # Из полученного в json-формате списка hh_vacancies получаем
            # список вакансий и список работодателей помощью функци create_vacancy_lists.
            employers_list, vacancies_list = create_vacancy_lists(hh_vacancies, employers_list, vacancies_list)
            page += 1
        else:
            print(f'Ответ: {hh_api.get_status_code()} - не удалось получить доступ к ресурсу HeadHater.')
            break
    return employers_list, vacancies_list


def create_vacancy_lists(hh_vacancies, employers_list, vacancies_list) -> (list, list):
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

            # валидация зарплаты salary_valid и должностных обязанностей responsibility_valid
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
        return employers_list, vacancies_list
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


def select_vacancies_list(employers_list, vacancies_list):
    """Функция сортирует список работодателей по убыванию максимальной заплаты и берет десять первых и формирует
    новый список selected_employers_list. Далее из списка ваканасий создается новый список вакансий
    selected_employers_list по соответствию ID вакансий в двух списках vac[8] == sel[0]."""
    for vac in vacancies_list:
        for sel in selected_employers_list:
            if vac[8] == sel[0]:
                selected_vacancies_list.append(vac)
    return selected_employers_list, selected_vacancies_list


def save_list_file(selected_employers_list, employer_list_file):
    print(employer_list_file)
    """Запись списка txt-файл."""
    with open(employer_list_file, 'w', encoding="UTF-8") as file:
        for item in selected_employers_list:
            file.write("%s\n" % item)
