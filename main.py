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
    search_word = search_config["employers_count"]           # поисковое слово для запроса на HeadHanter
    count_employers = int(search_config["employers_count"])  # количество интересующих работодателей
    print(f'{search_config["employers_count"]} интересующих работодателей будут выбраны по ключевому слову '
          f'"{search_config["search_word"]}".')
    page_quantity = input(f'Введите количество страниц (не более 20):\n')
    # параметры поиска на HeadHanter
    params = {'text': search_word, 'area': '113', 'currency': 'RUR', 'per_page': 100, 'page': 0}
    if not page_quantity.isdigit() or int(page_quantity) > 20:
        print('Неверный ввод - по умолчанию будет выбраны 2 страницы!')
        page_quantity = '2'
    if not os.path.isfile(EMPLOYERS_FILE):
        # в случае отсуствия файла с работодателями он формируется по запросу к HeadHanter из вакансий.
        # read_employers_list - функция для формирования списка работодателей.
        selected_employers = read_employers_lists(params, int(page_quantity), URL_GET)
        if len(selected_employers) > 0:
            # в окончательный список после сортировки берем только count_employers вакансий с наивысшей зарплатой.
            selected_emp_10 = sorted(selected_employers, key=lambda x: x[3], reverse=True)[0:count_employers]
            # сохраняем работодателей в json-файле.
            save_json_file(selected_emp_10, EMPLOYERS_FILE)
            # на базе полученных работодателей формируем соовествующий им список вакансий.
            selected_vac = read_vacancies_lists(params, int(page_quantity), selected_emp_10, URL_GET)
            database_manager(selected_emp_10, selected_vac)
        else:
            print('По запросу ничего не найдено!')
    else:
        # в случае если json-файл с работодателями уже есть, то загружаем их из файла.
        selected_emp_10 = load_json_file(EMPLOYERS_FILE)
        # на базе загруженных работодателей формируем сооветствующий им список вакансий.
        selected_vac = read_vacancies_lists(params, int(page_quantity), selected_emp_10, URL_GET)
        database_manager(selected_emp_10, selected_vac)


if __name__ == '__main__':
    users_menu()
