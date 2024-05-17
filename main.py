# Модуль запуска программы для ввода параметров подключения к БД, ввода количества страниц данных для поиска
# на HeadHanter и поисковых параметров для запроса к БД.

from src.functions import read_vacancies_lists
from src.functions import read_employers_lists
from src.functions import save_json_file
from src.functions import load_json_file
from src.functions import database_manager
from config import ROOT_DIR
import configparser
import os

EMPLOYERS_FILE = ROOT_DIR+'/employers_file.json'  # файл с списком выбранных работодателей.
URL_GET = "https://api.hh.ru/vacancies"  # адрес HeadHanter для отправки запроса.
SEARCH_WORD = "oracle"  # поисковое слово для запроса на HeadHanter.
COUNT_EMPLOYERS = 10  # количество интересующих работодателей.
SEARCH_CONFIGURATION_FILE = ROOT_DIR+'/search_parameters.ini'  # конфигурационный файл для параметров поиска.


def users_menu():
    """ Функция выполняет чтение и парсинг файла конфигурации поисковых параметров. Запарашивает количество загружаемых
     страниц HeadHanter и формирует список работодателей с наибольшей зарплатой и запоминает их в json-файле.
     При повторном запуске программы для дальнейшей работы берется уже сущуствующий файл с работодателями.
     Далее из HeadHanter загружаются вакансии, соответствующие выбранным работодателям. Работа функции завершается
     запуском менеджера-функци работы с БД PostgreSQL database_manager() для подключения, загрузки БД
     и выполнение запросов."""
    # парсинг конфигурационного файла с параметрами запроса.
    config = configparser.ConfigParser()
    config.read(SEARCH_CONFIGURATION_FILE)
    search_config = dict(config.items('search'))
    print(f'{search_config["employers_count"]} интересующих работодателей будут выбраны по ключевому слову '
          f'"{search_config["search_word"]}".')
    page_quantity = input(f'Введите количество страниц (не более 20):\n')
    if not page_quantity.isdigit() or int(page_quantity) > 20:
        print('Неверный ввод - по умолчанию будет выбраны 2 страницы!')
        page_quantity = '2'
    if not os.path.isfile(EMPLOYERS_FILE):
        # в случае отсуствия файла с работодателями он формируется по запросу к HeadHanter из вакансий.
        params = {'text': SEARCH_WORD, 'area': '113', 'currency': 'RUR', 'per_page': 100, 'page': 0}
        # read_employers_list - функция для формирования списка работодателей selected_employers
        selected_employers = read_employers_lists(params, int(page_quantity), URL_GET)
        if len(selected_employers) > 0:
            # в окончательный список после сортировки берем только COUNT_EMPLOYERS вакансий с наивысшей зарплатой.
            selected_emp_10 = sorted(selected_employers, key=lambda x: x[3], reverse=True)[0:COUNT_EMPLOYERS]
            # сохраняем работодателей в json-файле.
            save_json_file(selected_emp_10, EMPLOYERS_FILE)
            # на базе полученных работодателей формируем соовествующий им список вакансий.
            selected_vac = read_vacancies_lists(params, int(page_quantity), selected_emp_10, URL_GET)
            database_manager(selected_emp_10, selected_vac)
        else:
            print('По запросу ничего не найдено!')
    else:
        # функция select_employers_list предназначена для получения списка 10-ти организаций с наибольшей зарплатой.
        # функция connector считывает файл с параметрами подключения к БД возвращает данные в виде словаря.
        params = {'text': SEARCH_WORD, 'area': '113', 'currency': 'RUR', 'per_page': 100, 'page': 0}
        # в случае если json-файл с работодателями уже есть, то загружаем их из файла.
        selected_emp_10 = load_json_file(EMPLOYERS_FILE)
        # на базе загруженных работодателей формируем соовествующий им список вакансий.
        selected_vac = read_vacancies_lists(params, int(page_quantity), selected_emp_10, URL_GET)
        database_manager(selected_emp_10, selected_vac)


if __name__ == '__main__':
    users_menu()
