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


class DataBaseManager(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def create_database(self):
        pass

    @abstractmethod
    def connect_database(self):
        pass

    @abstractmethod
    def create_tables(self, selected_emp, selected_vac):
        pass
