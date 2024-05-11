from src.functions import read_vacancies_list
from src.functions import select_vacancies_list
from src.class_db_manager import DBManager

URL_GET = "https://api.hh.ru/vacancies"  # адрес HH для отправки запроса
CONNECTION_PARAMETERS = {
    "database": "hh_vacancies",
    "user": "postgres",
    "password": "12345",
    "host": "127.0.0.1",
    "port": "5432"
}


def users_menu():
    # vacancy_text - текст запроса на HeadHanter
    vacancy_text = input(f'Введите поисковый запрос:\n')
    if len(vacancy_text) > 0:
        # параметры запроса
        params = {'text': vacancy_text, 'area': '113', 'currency': 'RUR', 'per_page': 100, 'page': 0}
        page_quantity = input(f'Введите количество страниц (не более 20):\n')
        if not page_quantity.isdigit() or int(page_quantity) > 20:
            print('Неверный ввод - будет выбрана одна страница!')
            page_quantity = '1'
        text_region = input(f'Введите наименование региона или нажмите <Enter> если поиск по всей России:\n')
        if len(text_region) == 0:
            print('Регион не введен - поиск будет осуществляться по всей России.')

        # read_vacancies_list - функция для формирования
        # списка работодателей selected_employers и вакансий vacancies_list
        selected_employers, selected_vacancies = read_vacancies_list(params, int(page_quantity), text_region, URL_GET)

        if len(selected_vacancies) > 0:
            selected_emp, selected_vac = select_vacancies_list(selected_employers, selected_vacancies)
            print(len(selected_emp))
            print(len(selected_vac))
            db_manager = DBManager(CONNECTION_PARAMETERS)
            db_manager.create_database()
            db_manager.create_tables(selected_emp)
        else:
            print('По запросу ничего не найдено!')
    else:
        print('Запрос не введен - программа прекращает работу!')


if __name__ == '__main__':
    users_menu()
