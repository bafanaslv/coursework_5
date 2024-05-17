# Модуль запуска программы для ввода параметров подключения к БД, ввода количества страниц данных для поиска
# на HeadHanter и поисковых параметров для запроса к БД.

from src.functions import read_vacancies_list
from src.functions import read_employers_list
from src.functions import save_json_file
from src.functions import load_json_file
from src.functions import database_manager
from config import ROOT_DIR
import os.path

EMPLOYERS_FILE = ROOT_DIR+'/data/employer_file.json'  # файл с списком выбранных работодателей
URL_GET = "https://api.hh.ru/vacancies"  # адрес HeadHanter для отправки запроса
SEARCH_WORD = "oracle"  # поисковое слово для запроса на HeadHanter
COUNT_EMPLOYERS = 10  # количество интересующих работодателей

def users_menu():
    page_quantity = input(f'Введите количество страниц (не более 20):\n')
    if not page_quantity.isdigit() or int(page_quantity) > 20:
        print('Неверный ввод - по умолчанию будет выбраны две страницы!')
        page_quantity = '2'
    if not os.path.isfile(EMPLOYERS_FILE):
        # vacancy_text - текст запроса на HeadHanter
        params = {'text': SEARCH_WORD, 'area': '113', 'currency': 'RUR', 'per_page': 100, 'page': 0}
        # read_employers_list - функция для формирования списка работодателей selected_employers
        selected_employers = read_employers_list(params, int(page_quantity), URL_GET)
        if len(selected_employers) > 0:
            selected_emp_10 = sorted(selected_employers, key=lambda x: x[3], reverse=True)[0:COUNT_EMPLOYERS]
            save_json_file(selected_emp_10, EMPLOYERS_FILE)
            selected_vac = read_vacancies_list(params, int(page_quantity), selected_emp_10, URL_GET)
            database_manager(selected_emp_10, selected_vac)
        else:
            print('По запросу ничего не найдено!')
    else:
        # функция select_employers_list предназначена для получения списка 10-ти организаций с наибольшей зарплатой.
        # функция connector считывает файл с параметрами подключения к БД возвращает данные в виде словаря.
        params = {'text': SEARCH_WORD, 'area': '113', 'currency': 'RUR', 'per_page': 100, 'page': 0}
        selected_emp_10 = load_json_file(EMPLOYERS_FILE)
        selected_vac = read_vacancies_list(params, int(page_quantity), selected_emp_10, URL_GET)
        database_manager(selected_emp_10, selected_vac)


if __name__ == '__main__':
    users_menu()
