# Модуль запуска программы для ввода параметров подключения к БД, ввода данных для поиска на HeadHanter
# и вывода меню запросов к БД

from src.functions import read_vacancies_list
from src.functions import read_employers_list
from src.functions import save_list_file
from src.functions import database_manager
from config import ROOT_DIR
import os.path

EMPLOYERS_LIST_FILE = ROOT_DIR+'/data/employer_list_file.csv'  # файл с списком выбранных работодателей
URL_GET = "https://api.hh.ru/vacancies"  # адрес HeadHanter для отправки запроса


def users_menu():
    selected_emp_10 = []
    selected_vac = []
    page_quantity = input(f'Введите количество страниц (не более 20):\n')
    if not page_quantity.isdigit() or int(page_quantity) > 20:
        print('Неверный ввод - по умолчанию будет выбрана одна страница!')
        page_quantity = '1'
    if not os.path.isfile(EMPLOYERS_LIST_FILE):
        # vacancy_text - текст запроса на HeadHanter
        vacancy_text = input(f'Введите поисковый запрос:\n')
        if len(vacancy_text) > 0:
            # параметры запроса
            params = {'text': vacancy_text, 'area': '113', 'currency': 'RUR', 'per_page': 100, 'page': 0}
            # read_employers_list - функция для формирования списка работодателей selected_employers
            selected_employers = read_employers_list(params, int(page_quantity), URL_GET)
            if len(selected_employers) > 0:
                selected_emp_10 = sorted(selected_employers, key=lambda x: x[3], reverse=True)[0:10]
                save_list_file(selected_emp_10, EMPLOYERS_LIST_FILE)
                selected_vac = read_vacancies_list(params, int(page_quantity), selected_emp_10, URL_GET)
                database_manager(selected_emp_10, selected_vac)
            else:
                print('По запросу ничего не найдено!')
        else:
            print('Запрос не введен - программа завершает работу !')
    else:
        # функция select_employers_list предназначена для получения списка 10-ти организаций с наибольшей зарплатой.
        # функция connector считывает файл с параметрами подключения к БД возвращает данные в виде словаря.
        params = {'text': '', 'area': '113', 'currency': 'RUR', 'per_page': 100, 'page': 0}
        with open(EMPLOYERS_LIST_FILE, 'r', encoding="UTF-8") as file:
            selected_emp_10 = file.readlines()
        print(selected_emp_10)
        selected_vac = read_vacancies_list(params, int(page_quantity), selected_emp_10, URL_GET)
        database_manager(selected_emp_10, selected_vac)


if __name__ == '__main__':
    users_menu()
