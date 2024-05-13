import psycopg2
from psycopg2 import Error
from src.class_abstract import DataBaseManager


class DBManager(DataBaseManager):
    """ Класс предназначен для работы с БД PostgeSQL и содержит методы подключения, создания БД и таблиц.
    после созднания таблиц выполняется загрузка данных в таблицы из данных полученных с HeadHanter."""
    def __init__(self, connection_parameters):  # connection_parameters - параметры подключения.
        self.database = connection_parameters["database"]
        self.user = connection_parameters["user"]
        self.password = connection_parameters["password"]
        self.host = connection_parameters["host"]
        self.port = connection_parameters["port"]
        self.connection = None  # Идентификатор подключения к БД.
        self.cursor = None      # Курсор для выполнения операций с БД.
        self.error = None       # Флаг для возврата результатов выполнения запросов к БД.

    def create_database(self) -> bool:
        """ Данный метод предназначен для подключения к PostgreSQL и пересоздания пустой БД hh_vacancies."""
        self.error = True
        try:
            # Подключение к существующей базе данных
            self.connection = psycopg2.connect(user=self.user,
                                               password=self.password,
                                               host=self.host,
                                               port=self.port)
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

    def connect_database(self) -> bool:
        """ Данный метод предназначен для создания подключения к нашей БД hh_vacancies."""
        self.error = True
        try:
            self.connection = psycopg2.connect(user=self.user,
                                               password=self.password,
                                               host=self.host,
                                               port=self.port,
                                               database=self.database)
            self.cursor = self.connection.cursor()
            # autocommit - флаг для управления изменениями в БД (True - автоматическое выполнение COMMIT)
            self.connection.autocommit = True
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
            self.error = False
        return self.error

    def create_tables(self, selected_emp, selected_vac) -> bool:
        """ Метод предназначен для пересоздпния таблиц employers и vacancies
        и загрузки их из списков selected_emp, selected_vac."""
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
                data_ins = data[0:3]  # отсекаем технологическое поле для сортировки списка.
                self.cursor.execute(
                    "INSERT INTO employers (employer_id, name, alternate_url) "
                    "VALUES (%s, %s, %s) returning *", data_ins)
            for data in selected_vac:
                # избавляемся от ненужных символов в столбцах name, requirement, responsibility.
                data[1] = data[1].replace("<highlighttext>", "").replace("</highlighttext>", "")
                if data[3] is not None:
                    data[3] = data[3].replace("<highlighttext>", "").replace("</highlighttext>", "")
                if data[4] is not None:
                    data[4] = data[4].replace("<highlighttext>", "").replace("</highlighttext>", "")
                self.cursor.execute(
                    "INSERT INTO vacancies (vacancy_id, name, area, requirement, responsibility, "
                    "salary_min, salary_max, currency, employer_id)"
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) returning *", data)
            print(f"Таблицы базаы данных {self.database} employer_id и vacancies созданы и загружены.\n")
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
            self.error = False
        return self.error

    def get_companies_and_vacancies_count(self) -> bool:
        """ Метод предназначен для получения списка компаний с количеством вакансий. """
        self.error = True
        try:
            self.cursor.execute("SELECT employers.name, COUNT(*) "
                                "FROM vacancies "
                                "INNER JOIN employers USING(employer_id) "
                                "GROUP BY employers.name")
            selected_rows = self.cursor.fetchall()
            n = 1
            for row in selected_rows:
                print(f"{n}.Компания: {row[0]}, количество вакансий: {row[1]}")
                n += 1
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
            self.error = False
        return self.error

    def get_all_vacancies(self):
        """ Метод предназначен для получения списка всех вакансий."""
        self.error = True
        try:
            self.cursor.execute("SELECT employers.name, area, vacancies.name, "
                                "salary_min, salary_max, currency, alternate_url "
                                "FROM vacancies "
                                "INNER JOIN employers USING(employer_id) "
                                "ORDER BY employers.name")
            selected_rows = self.cursor.fetchall()
            n = 1
            for row in selected_rows:
                if row[3] == 0 and row[4] == 0:
                    print(f"{n}.Компания: {row[0]} {row[1]}, вакансия: {row[2]}, зарплата не указана")
                elif row[3] > 0 and row[4] == 0:
                    print(f"{n}.Компания: {row[0]} {row[1]}, вакансия: {row[2]}, зарплата (мин): {row[3]} {row[5]}")
                elif row[3] == 0 and row[4] > 0:
                    print(f"{n}.Компания: {row[0]} {row[1]}, вакансия: {row[2]}, зарплата (мах): {row[4]} {row[5]}")
                else:
                    print(f"{n}.Компания: {row[0]} {row[1]}, вакансия: {row[2]}, зарплата: {row[3]}-{row[4]} {row[5]}")
                n += 1
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
            self.error = False
        return self.error

    def get_avg_salary(self) -> bool:
        """ Метод предназначен для получения средней зарплаты по всем рублевым вакансиям. В случае, если вакансия
        содержит и минимальную и максимальную зарплату, то для выполнения запроса применяем среднее значение."""
        self.error = True
        try:
            self.cursor.execute("SELECT AVG((salary_min+salary_max)/"
                                "CASE WHEN salary_min > 0 AND salary_max > 0 THEN 2 ELSE 1 END) "
                                "AS avg_salary FROM vacancies "
                                "WHERE (salary_min > 0 OR salary_max > 0) AND currency = 'RUR'")
            print(f"Средняя зарплата по всем компаниям: {int(self.cursor.fetchone()[0])} руб.")
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
            self.error = False
        return self.error

    def get_vacancies_with_higher_salary(self):
        """ Метод предназначен для получения списка всех вакансий, у которых зарплата выше средней по всем рублевым
        вакансиям. В случае, если вакансия содержит и минимальную и максимальную зарплату,то для выполнения запроса
        применяем среднее значение."""
        self.error = True
        try:
            self.cursor.execute("SELECT AVG((salary_min+salary_max)/"
                                "CASE WHEN salary_min > 0 AND salary_max > 0 THEN 2 ELSE 1 END) "
                                "AS avg_salary FROM vacancies "
                                "WHERE (salary_min > 0 OR salary_max > 0) AND currency = 'RUR'")
            print(f'Список вакансий у которых зарплата выше средней: {int(self.cursor.fetchone()[0])} руб.\n')
            self.cursor.execute("SELECT employers.name, area, vacancies.name, salary_min,salary_max, currency "
                                "FROM vacancies INNER JOIN employers USING(employer_id) "
                                "WHERE currency = 'RUR' AND (salary_min+salary_max)/"
                                "CASE WHEN salary_min > 0 AND salary_max > 0 THEN 2 "
                                "ELSE 1 END > (SELECT AVG((salary_min+salary_max)/"
                                "CASE WHEN salary_min > 0 AND salary_max > 0 THEN 2 "
                                "ELSE 1 END) AS avg_salary FROM vacancies WHERE salary_min > 0 OR salary_max > 0)")
            selected_rows = self.cursor.fetchall()
            n = 1
            for row in selected_rows:
                if row[3] > 0 and row[4] == 0:
                    print(f"{n}.Компания: {row[0]} {row[1]}, вакансия: {row[2]}, зарплата (мин): {row[3]} руб.")
                elif row[3] == 0 and row[4] > 0:
                    print(f"{n}.Компания: {row[0]} {row[1]}, вакансия: {row[2]}, зарплата (мах): {row[4]} руб.")
                else:
                    print(f"{n}.Компания: {row[0]} {row[1]}, вакансия: {row[2]}, зарплата: {row[3]} - {row[4]} руб.")
                n += 1
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
            self.error = False
        return self.error

    def get_vacancies_with_keyword(self, keyword_str) -> bool:
        """ Метод предназначен для получения всех вакансий, в названии которых содержатся переданные в метод слова."""
        self.error = True
        try:
            self.cursor.execute(f"SELECT employers.name, vacancies.name, area "
                                "FROM vacancies "
                                "INNER JOIN employers USING(employer_id) "
                                f"WHERE lower(vacancies.name) LIKE ANY (ARRAY{keyword_str})"
                                "ORDER BY employers.name")
            selected_rows = self.cursor.fetchall()
            if len(selected_rows) > 0:
                n = 1
                for row in selected_rows:
                    print(f"{n}.Компания: {row[0]}, вакансия: {row[1]}, г.{row[2]}")
                    n += 1
            else:
                key_words = keyword_str.replace("%', '%", "', '").replace("['%", "").replace("%']", "")
                print(f"Ни одна вакансия не содержит слова(строки) '{key_words}'.")
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
            self.error = False
        return self.error

    def close_database(self):
        """ Метод предназначен для закрытия подключения к БД."""
        self.cursor.close()
        self.connection.close()
        print("Соединение с PostgreSQL закрыто\n")
