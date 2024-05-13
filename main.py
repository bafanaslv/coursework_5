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
            db_manager = DBManager(CONNECTION_PARAMETERS)
            if db_manager.create_database() and db_manager.connect_database() and db_manager.create_tables(selected_emp, selected_vac):
                print("1.Получить список компаний с количеством вакансий\n"
                      "2.получить список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на ваканси\n"
                      "3.Получить среднюю зарплату по вакансиямт\n"
                      "4.Получить список всех вакансий, у которых зарплата выше средней по всем вакансиям\n"
                      "5.Получить список всех вакансий, в названии которых содержатся переданные в метод слова\n"
                      "  Выход - любой символ или <Enter>")
                answer = input()  # ввод номера опции выбора
                if answer == '1':
                    db_manager.get_companies_and_vacancies_count()
                elif answer == '2':
                    db_manager.get_all_vacancies()
                elif answer == '3':
                    db_manager.get_avg_salary()
                elif answer == '4':
                    db_manager.get_vacancies_with_higher_salary()
                elif answer == '5':
                    keyword = input("Введите ключевое слово в названии вакансии:\n")
                    if len(keyword) > 0:
                        db_manager.get_vacancies_with_keyword(keyword)
                    else:
                        print("Ключевое слово не введено - программа завершает работу !")
                else:
                    print('Не выбрана ни одна опция !')
                db_manager.close_database()
        else:
            print('По запросу ничего не найдено!')
    else:
        print('Запрос не введен - программа завершает работу !')


if __name__ == '__main__':
    users_menu()
