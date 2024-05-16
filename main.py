# Модуль запуска программы для ввода параметров подключения к БД, ввода данных для поиска на HeadHanter
# и вывода меню запросов к БД

from src.functions import read_vacancies_list
from src.functions import select_vacancies_list
from src.functions import save_list_file
from src.class_db_manager import DBManager
from config import ROOT_DIR
from src.functions import connector
import os.path

CONNECTION_FILE = ROOT_DIR+'/database.ini'  # конфигурационный файл для подключения к БД
EMPLOYERS_LIST_FILE = ROOT_DIR+'/data/employer_list_file.txt'  # файл с списком выбранных работодателей
URL_GET = "https://api.hh.ru/vacancies"  # адрес HeadHanter для отправки запроса


def users_menu():
    # vacancy_text - текст запроса на HeadHanter
    os.path.isfile(EMPLOYERS_LIST_FILE)
    vacancy_text = input(f'Введите поисковый запрос:\n')
    if len(vacancy_text) > 0:
        # параметры запроса
        params = {'text': vacancy_text, 'area': '113', 'currency': 'RUR', 'per_page': 100, 'page': 0}
        page_quantity = input(f'Введите количество страниц (не более 20):\n')
        if not page_quantity.isdigit() or int(page_quantity) > 20:
            print('Неверный ввод - по умолчанию будет выбрана одна страница!')
            page_quantity = '1'

        # read_vacancies_list - функция для формирования
        # списка работодателей selected_employers и вакансий selected_vacancies
        selected_employers, selected_vacancies = read_vacancies_list(params, int(page_quantity), URL_GET)

        if len(selected_vacancies) > 0:
            # функция select_employers_list предназначена для получения списка 10-ти организаций с наибольшей зарплатой.
            # функция connector считывает файл с параметрами подключения к БД возвращает данные в виде словаря.
            selected_emp = sorted(selected_employers, key=lambda x: x[3], reverse=True)[0:10]
            save_list_file(selected_emp, EMPLOYERS_LIST_FILE)
            selected_vac = select_vacancies_list(selected_emp, selected_vacancies)

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
            elif not db_manager.create_tables(selected_emp, selected_vac):
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
        else:
            print('По запросу ничего не найдено!')
    else:
        print('Запрос не введен - программа завершает работу !')


if __name__ == '__main__':
    users_menu()
