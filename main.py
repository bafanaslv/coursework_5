from config import ROOT_DIR
from src.functions import create_vacancies_list
from src.class_json_saver import JSONSaver
from src.class_vacancy import Vacancy

JSON_FILE = ROOT_DIR+'/data/vacancies.json'  # json-файл с вакансиями
MY_JSON_FILE = ROOT_DIR+'/data/my_vacancies.json'  # json-файл с моими вакасиями для добавления
URL_GET = "https://api.hh.ru/vacancies"  # адрес HH для отправки запроса


def users_menu():
    # vacancy_text - текст запроса на Head Hanter
    vacancy_text = input(f'Введите поисковый запрос:\n')
    if len(vacancy_text) > 0:
        # параметры запроса
        params = {'text': vacancy_text, 'area': '113', 'currency': 'RUR', 'per_page': 100, 'page': 0}
        page_quantity = input(f'Введите количество страниц (не более 20):\n')
        if not page_quantity.isdigit() or int(page_quantity) > 20:
            print('Неверный ввод - выбрана одна страница!')
            page_quantity = '1'
        # create_vacancies_list - функция для формирования списка объектов вакансий vacancies_objects_list
        vacancies_objects_list = create_vacancies_list(params, int(page_quantity), URL_GET)

        # Создаем объект manager и одновременно получаем список словарей вакансий в новом усеченном формате и
        # сохраняем его а json-файле JSON_FILE
        json_manager = JSONSaver(vacancies_objects_list, JSON_FILE)

        if len(vacancies_objects_list) > 0:
            print("1.Вывести все вакансии\n"
                  "2.Получить топ N вакансий по нижнему уровню зарплаты\n"
                  "3.Получить топ N вакансий по верхнему уровню зарплаты\n"
                  "4.Получить вакансии по дипазону зарплат\n"
                  "5.Получить вакансии по региону\n"
                  "6.Получить вакансии с ключевым словом в описании\n"
                  "7.Удалить из json-файла выбранный регион\n"
                  "8.Добавить к json-файлу свои вакансии из другого json-файла\n"
                  "  Выход - любой символ или <Enter>")
            answer = input()  # ввод номера опции выбора
            if answer not in ['1', '2', '3', '4', '5', '6', '7', '8']:
                print('Не выбрана ни одна опция !')
            elif answer == '1':
                # Вывод всех найденных вакансий.
                json_manager.print_vacancies(vacancies_objects_list)
            elif answer == '2' or answer == '3':
                # Выборка топ N вакансий по нижнему уровню зарплаты и вывод.
                if answer == '2':
                    text = input(f'Введите количество топ вакансий по нижней границе зарплаты:\n')
                else:
                    text = input(f'Введите количество топ вакансий по верхней границе зарплаты:\n')
                if text.isdigit():
                    sel_obj_list = json_manager.vacancies_top_salary(vacancies_objects_list, text, JSON_FILE, answer)
                    json_manager.print_vacancies(sel_obj_list)
                else:
                    print('Необходимо ввести целое положительное число.')
            elif answer == '4':
                # Выборка вакансий по дипазону зарплат.
                sal_min = input(f'Введите минимальную зарплату:\n')
                sal_max = input(f'Введите максимальную зарплату:\n')
                if sal_min.isdigit() and sal_max.isdigit() and int(sal_max) > int(sal_min):
                    # vac_object - объект-шаблон вакансия для сравнения зарплат.
                    vac_object = Vacancy("0", "Программист", "Казань", "Все",
                                         "И даже больше", int(sal_min), int(sal_max),
                                         "руб.", "Facebook", "htts:\\www.facebook.com")
                    sel_obj_list = json_manager.vacancies_range_salary(vacancies_objects_list, vac_object, JSON_FILE)
                    json_manager.print_vacancies(sel_obj_list)
                else:
                    if int(sal_max) <= int(sal_min):
                        print('Введен неверный диапазон зарплаты.')
                    else:
                        print('Необходимо ввести целые положительные числа.')
            elif answer == '5':
                # Выборка вакансий по региону и вывод.
                text = input(f'Введите нименование региона:\n')
                if len(text):
                    sel_obj_list = json_manager.select_vacancies_by_region(vacancies_objects_list, text, JSON_FILE)
                    json_manager.print_vacancies(sel_obj_list)
                else:
                    print('Регион поиска не введен.')
            elif answer == '6':
                # Выборка вакансий по ключевому слову и вывод.
                text = input(f'Введите ключевое слово:\n')
                if len(text):
                    sel_obj_list = json_manager.select_vacancies_by_word(vacancies_objects_list, text, JSON_FILE)
                    json_manager.print_vacancies(sel_obj_list)
                else:
                    print('Ключевое слово не введено.')
            elif answer == '7':
                # Выборка вакансий по региону и сохраннние а json-файл.
                text = input(f'Введите нименование региона, который хотите удалить из json-файла:\n')
                if len(text):
                    json_manager.del_vacancies(text, JSON_FILE)
                    print(f'Из файла {JSON_FILE} удален регион {text}.')
                else:
                    print('Регион для удаления не введен.')
            elif answer == '8':
                json_manager.add_vacancies(Vacancy.max_id, JSON_FILE, MY_JSON_FILE)
                print(f'К файлу {JSON_FILE} добавлен файл {MY_JSON_FILE}.')
        else:
            print('По запросу ничего не найдено!')
    else:
        print('Не выбрана ни одна опция !')


if __name__ == '__main__':
    users_menu()
