# JSONSaver класс предназначен для работы со списками объектов и словарей вакансий.

import json
from src.class_abstract import JsonManager
from src.class_vacancy import Vacancy


class JSONSaver(JsonManager):
    """Этот класс предназначен для работы со списками объектов и словарей вакансий."""
    def __init__(self, vacancies_objects_list, json_file):
        self.create_vacancies_list(vacancies_objects_list, json_file)  # подготовка списка вакансий

    def create_vacancies_list(self, vacancies, json_file) -> list:
        """Подготовка списка словарей вакансий my_vacancies_list из json-файла."""
        my_vacancies_list = []
        for vacancy in vacancies:
            vacancy_dict = {"id": vacancy.idv,
                            "name": vacancy.name,
                            "area": vacancy.area,
                            "requirement": vacancy.requirement,
                            "responsibility": vacancy.responsibility,
                            "salary_min": vacancy.salary_min,
                            "salary_max": vacancy.salary_max,
                            "currency": vacancy.currency,
                            "employer_id": vacancy.employer_id,
                            "employer": vacancy.employer,
                            "employer_url": vacancy.employer_url}
            my_vacancies_list.append(vacancy_dict)
        self.save_json_file(my_vacancies_list, json_file)  # запись в json-файл
        return my_vacancies_list

    def save_json_file(self, vacancies_dict_list, json_file):
        """Запись в json-файл."""
        with open(json_file, 'w', encoding="UTF-8") as file:
            vacancy_json = json.dumps(vacancies_dict_list, ensure_ascii=False, indent=4)
            file.write(vacancy_json)

    @staticmethod
    def load_json_file(json_file):
        """Загрузка json-файла."""
        with open(json_file, 'r', encoding='utf-8') as file:
            return json.load(file)

    def add_vacancies(self, max_id, json_file, my_json_file):
        """Добавление в json-файл вакансий, находящихся в json-файле my_json_file."""
        vacancies_list = self.load_json_file(json_file)
        my_vacancies_list = self.load_json_file(my_json_file)
        for new_vacancy in my_vacancies_list:
            max_id += 1
            new_vacancy["id"] = str(max_id)
            vacancies_list.append(new_vacancy)
            Vacancy.max_id += 1
        self.save_json_file(vacancies_list, json_file)

    def del_vacancies(self, area_name, json_file):
        """Удаление из json-файла ненужных вакансий региона area_name."""
        my_vacancies_list = []
        vacancies_list = self.load_json_file(json_file)
        for i in range(len(vacancies_list)):
            if vacancies_list[i]['area'] != area_name:
                my_vacancies_list.append(vacancies_list[i])
        self.save_json_file(my_vacancies_list, json_file)

    def vacancies_top_salary(self, vacancies_obj_list, top_n, json_file, answer) -> list:
        """Выборка топ top_n по зарплате. По нижней или верхней границе в зависимости от того какую опцию выбрал
        пользователь. answer == '2' - по минимальной, answer == '3' по максимальной"""
        # Предварительная сортировка по убыванию размера зарплаты.
        if answer == '2':
            vacancies_obj_list.sort(key=lambda x: x.salary_min, reverse=True)
        else:
            vacancies_obj_list.sort(key=lambda x: x.salary_max, reverse=True)

        # Фильтрация топ (top = top_n) вакансий или берем сколько нашлось (top = len(vacancies_dict_list)).
        my_vacancies_list = []
        if len(vacancies_obj_list) < int(top_n):
            top = len(vacancies_obj_list)
        else:
            top = int(top_n)
        i = 0
        while i < top:
            my_vacancies_list.append(vacancies_obj_list[i])
            i += 1
        return my_vacancies_list

    def vacancies_range_salary(self, vacancies_objects_list, vac_obj, json_file) -> list:
        """Выборка по дипазону зарплаты. Объект vac_object является эталоном для сравнения.
        Выборка считается удачной, если верхняя и нижняя границы зарплаты входят в дипазан
        или если верхняя или нижняя входят в дипазон в случае когда соотвествующая им пара не указана."""
        my_vacancies_list = []
        for i in range(len(vacancies_objects_list)):
            if self.__lte__(vacancies_objects_list[i], vac_obj) and self.__gte__(vacancies_objects_list[i], vac_obj):
                my_vacancies_list.append(vacancies_objects_list[i])
        return my_vacancies_list

    @staticmethod
    def __lte__(vacancy, other) -> bool:
        """Сравнение вхождения в диапазон нижней зарплаты. Если она не указана,
        то проверяем вхождение в диапазон верхней зарплаты."""
        if isinstance(other, Vacancy):
            if vacancy.salary_min > 0:
                return other.salary_min <= vacancy.salary_min <= other.salary_max
            else:
                if vacancy.salary_max > 0:
                    return other.salary_min <= vacancy.salary_max <= other.salary_max
        return False

    @staticmethod
    def __gte__(vacancy, other) -> bool:
        """Сравнение вхождения в диапазон верхней зарплаты. Если она не указана,
        то проверяем вхождение в диапазон нижней зарплаты."""
        if isinstance(other, Vacancy):
            if vacancy.salary_max > 0:
                return other.salary_min <= vacancy.salary_max <= other.salary_max
            else:
                if vacancy.salary_min > 0:
                    return other.salary_min <= vacancy.salary_min <= other.salary_max
        return False

    def select_vacancies_by_region(self, vacancies_objects_list, area_name, json_file) -> list:
        """Из списка объектов берем только если регион area_name совпадает с введенным."""
        my_vacancies_list = []
        for i in range(len(vacancies_objects_list)):
            if vacancies_objects_list[i].area.lower() == area_name.lower():
                my_vacancies_list.append(vacancies_objects_list[i])
        return my_vacancies_list

    def select_vacancies_by_word(self, vacancies_objects_list, word, json_file):
        """Из списка объектов берем только, если ключевое слово найдено
        в требованиях к соискателю или в должностных обязанностях."""
        my_vacancies_list = []
        for i in range(len(vacancies_objects_list)):
            if (word.lower() in vacancies_objects_list[i].requirement.lower() or
                    word.lower() in vacancies_objects_list[i].responsibility.lower()):
                my_vacancies_list.append(vacancies_objects_list[i])
        return my_vacancies_list

    @staticmethod
    def print_vacancies(vacancies_dict_list):
        """ Вывод выбранных вакансий."""
        for vacancy in vacancies_dict_list:
            print(vacancy)
        print(f'Найдено {len(vacancies_dict_list)} вакансий.\n')
