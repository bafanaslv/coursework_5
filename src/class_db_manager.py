import psycopg2
from psycopg2 import Error
from src.class_abstract import DataBaseManager


class DBManager(DataBaseManager):
    def __init__(self, connection_parameters):
        self.database = connection_parameters["database"]
        self.user = connection_parameters["user"]
        self.password = connection_parameters["password"]
        self.host = connection_parameters["host"]
        self.port = connection_parameters["port"]
        self.connection = None
        self.cursor = None
        self.error = None

    def create_database(self):
        self.error = True
        try:
            # Подключение к существующей базе данных
            self.connection = psycopg2.connect(user=self.user,
                                               password=self.password,
                                               host=self.host,
                                               port=self.port)
            # Курсор для выполнения операций с базой данных
            self.cursor = self.connection.cursor()
            self.connection.autocommit = True
            self.cursor.execute(f"DROP DATABASE IF EXISTS {self.database}")
            self.cursor.execute(f"CREATE DATABASE {self.database}")
            print(f"База данных {self.database} создана.")
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
            self.error = False
        finally:
            if self.connection is not None:
                self.cursor.close()
                self.connection.close()
                print("Соединение с PostgreSQL закрыто\n")
        return self.error

    def connect_database(self):
        self.error = True
        try:
            # Подключение к созданной базе данных self.database
            self.connection = psycopg2.connect(user=self.user,
                                               password=self.password,
                                               host=self.host,
                                               port=self.port,
                                               database=self.database)
            # Курсор для выполнения операций с базой данных
            self.cursor = self.connection.cursor()
            self.connection.autocommit = True
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
            self.error = False
        return self.error

    def create_tables(self, selected_emp, selected_vac):
        self.error = True
        try:
            self.cursor.execute("DROP TABLE IF EXISTS vacancies;"
                                "DROP TABLE IF EXISTS employers;"
                                "CREATE TABLE employers (employer_id int PRIMARY KEY, "
                                "name varchar(100) NOT NULL, alternate_url varchar(100) NOT NULL);"
                                "CREATE TABLE vacancies (vacancy_id int PRIMARY KEY, "
                                "name varchar(100) NOT NULL, area varchar(50) NOT NULL, "
                                "requirement text, responsibility text, salary_min int, salary_max int, "
                                "currency char(5), employer_id int NOT NULL, "
                                "FOREIGN KEY (employer_id) REFERENCES employers(employer_id))")
            for data in selected_emp:
                data_ins = data[0:3]
                self.cursor.execute(
                    "INSERT INTO employers (employer_id, name, alternate_url) "
                    "VALUES (%s, %s, %s) returning *", data_ins)
            for data in selected_vac:
                self.cursor.execute(
                    "INSERT INTO vacancies (vacancy_id, name, area, requirement, responsibility, "
                    "salary_min, salary_max, currency, employer_id)"
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) returning *", data)
            print(f"Таблицы базаы данных {self.database} employer_id и vacancies созданы и загружены.")
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
            self.error = False
        return self.error

    def get_companies_and_vacancies_count(self):
        self.error = True
        try:
            self.cursor.execute("SELECT employers.name, COUNT(*) "
                                "FROM vacancies "
                                "INNER JOIN employers USING(employer_id) "
                                "GROUP BY employers.name ")
            selected = self.cursor.fetchall()
            for sel in selected:
                print(f'Компания: {sel[0]}, количество вакансий: {sel[1]}')
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
            self.error = False
        return self.error

    def close_database(self):
        self.cursor.close()
        self.connection.close()
        print("Соединение с PostgreSQL закрыто\n")
