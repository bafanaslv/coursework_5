from abc import ABC, abstractmethod


class HHapiABC(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def get_status_code(self):
        pass

    @abstractmethod
    def get_vacancies(self, url_get, params):
        pass


class JsonManager(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def save_json_file(self, vacancies, json_file):
        pass

    @abstractmethod
    def add_vacancies(self, max_id, my_json_file, json_file):
        pass

    @abstractmethod
    def del_vacancies(self, area_name, json_file):
        pass

    @abstractmethod
    def select_vacancies_by_region(self, vacancies_dict_list, area_name, json_file):
        pass

    @abstractmethod
    def select_vacancies_by_word(self, vacancies_dict_list, word, json_file):
        pass

    @abstractmethod
    def vacancies_top_salary(self, vacancies_dict_list, top_n, json_file, answer):
        pass

    @abstractmethod
    def vacancies_range_salary(self, vacancies_objects_list, vac_object, json_file):
        pass
